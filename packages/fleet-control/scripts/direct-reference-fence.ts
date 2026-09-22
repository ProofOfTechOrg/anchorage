// SPDX-License-Identifier: Apache-2.0

import type { ExecutionFenceState as FenceState } from '@proofoftech/flowsafe/do-runner';
import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import { DIRECT_TENANT_ROUTES } from './direct-credentialed-tenant-object.mjs';
import type { DirectReferenceContext } from './direct-reference-context.js';
import type { DirectReferenceAction } from './direct-reference-contract.mjs';
import { DirectReferenceExecutionError } from './direct-reference-http.js';
import {
  decodeDirectJsonObject,
  readBoundedDirectResponse,
} from './direct-reference-transport.js';

export type DirectFenceReading = {
  state: FenceState;
  mutationEpoch: number;
  requireMutationEpoch: boolean;
  transitionRevision: number;
};

export type DirectFenceSweep = {
  fence: DirectFenceReading;
  categories: { category: string; class: string; empty: boolean }[];
  observedAt: number;
};

/** The epoch label each probe operation sends to the tenant. */
const PROBE_EPOCHS = Object.freeze({
  'probe-missing': 'missing',
  'probe-stale': 'stale',
  'probe-future': 'future',
});

/** Membership in `PROBE_EPOCHS`, and the narrowing its index needs. */
const isProbeOperation = (value: string): value is keyof typeof PROBE_EPOCHS =>
  Object.hasOwn(PROBE_EPOCHS, value);
const isVersionedOperation = (
  value: string,
): value is 'drain' | 'reopen' | 'lock' | 'unlock' =>
  value === 'drain' ||
  value === 'reopen' ||
  value === 'lock' ||
  value === 'unlock';

type FenceTransitionResult =
  | { ok: true; after: DirectFenceReading }
  | {
      ok: false;
      reason: {
        code: 'FENCE_CAS_CONFLICT';
        state: FenceState;
        mutationEpoch?: number;
        requireMutationEpoch?: boolean;
        transitionRevision?: number;
        conflict?: 'expectation-mismatch' | 'versioned-expectation-required';
      };
    };

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new DirectReferenceExecutionError();
  return value as Record<string, unknown>;
}

function state(value: unknown): FenceState {
  if (
    value !== 'open' &&
    value !== 'draining' &&
    value !== 'migration-locked' &&
    value !== 'proof-only'
  )
    throw new DirectReferenceExecutionError();
  return value;
}

function counter(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
    throw new DirectReferenceExecutionError();
  return value;
}

function flag(value: unknown): boolean {
  if (typeof value !== 'boolean') throw new DirectReferenceExecutionError();
  return value;
}

function text(value: unknown): string {
  if (typeof value !== 'string') throw new DirectReferenceExecutionError();
  return value;
}

function reading(value: Record<string, unknown>): DirectFenceReading {
  return {
    state: state(value.state),
    mutationEpoch: counter(value.mutationEpoch),
    requireMutationEpoch: flag(value.requireMutationEpoch),
    transitionRevision: counter(value.transitionRevision),
  };
}

function transition(
  status: number,
  value: Record<string, unknown>,
): FenceTransitionResult {
  if (status === 200) return { ok: true, after: reading(value) };
  const reason = object(value.reason);
  if (
    reason.code !== 'FENCE_CAS_CONFLICT' ||
    (reason.conflict !== undefined &&
      reason.conflict !== 'expectation-mismatch' &&
      reason.conflict !== 'versioned-expectation-required')
  )
    throw new DirectReferenceExecutionError();
  return {
    ok: false,
    reason: {
      code: 'FENCE_CAS_CONFLICT',
      state: state(reason.state),
      ...(reason.mutationEpoch === undefined
        ? {}
        : { mutationEpoch: counter(reason.mutationEpoch) }),
      ...(reason.requireMutationEpoch === undefined
        ? {}
        : { requireMutationEpoch: flag(reason.requireMutationEpoch) }),
      ...(reason.transitionRevision === undefined
        ? {}
        : { transitionRevision: counter(reason.transitionRevision) }),
      ...(reason.conflict === undefined ? {} : { conflict: reason.conflict }),
    },
  };
}

export async function dispatchDirectFence(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  action: Extract<DirectReferenceAction, { kind: 'tenant-fence' }>,
  invocationSignal: AbortSignal,
): Promise<unknown> {
  const record = await context.control.getDeployment(
    manifest.names.roles[action.role].tenantTag,
    manifest.environment,
  );
  if (!record || context.roleFor(record) !== action.role)
    throw new DirectReferenceExecutionError();
  const spec = context.specFor(record);
  if (!spec.routeHostname) throw new DirectReferenceExecutionError();
  const { operation } = action;
  const application =
    operation === 'mutate-current' || isProbeOperation(operation);
  const secrets = context.secrets(action.role);
  const supplied = application
    ? secrets.application?.APP_PROBE_TOKEN
    : secrets.maintenanceAdmin;
  if (typeof supplied !== 'string' || !supplied)
    throw new DirectReferenceExecutionError();
  const token: string = supplied;

  async function request(path: string, body?: unknown) {
    const { status, text: encoded } = await readBoundedDirectResponse({
      fetch: application
        ? context.transport.applicationFetch
        : context.transport.maintenanceFetch,
      url: new URL(path, `https://${spec.routeHostname}`),
      method: body === undefined ? 'GET' : 'POST',
      token,
      body,
      acceptStatuses: isVersionedOperation(operation) ? [200, 409] : [200],
      mediaType: 'application/json',
      byteLimit: operation === 'inventory' ? 65536 : 4096,
      invocationSignal,
      requestTimeoutMs: context.transport.effectiveRequestTimeoutMs,
    });
    return { status, value: decodeDirectJsonObject(encoded) };
  }

  if (operation === 'read')
    return reading((await request('/admin/execution-fence')).value);
  if (isVersionedOperation(operation)) {
    if (!('expectedMutationEpoch' in action))
      throw new DirectReferenceExecutionError();
    const { expectedMutationEpoch, expectedRevision } = action;
    const result = await request(
      '/admin/execution-fence',
      operation === 'drain' || operation === 'lock'
        ? {
            expected: 'open',
            next: operation === 'drain' ? 'draining' : 'migration-locked',
            expectedMutationEpoch,
            expectedRevision,
            advanceMutationEpoch: true,
          }
        : {
            expected: operation === 'reopen' ? 'draining' : 'migration-locked',
            next: 'open',
            expectedMutationEpoch,
            expectedRevision,
          },
    );
    return transition(result.status, result.value);
  }
  if (operation === 'inventory') {
    const fence = reading((await request('/admin/execution-fence')).value);
    const index = (await request('/admin/inventory')).value;
    if (!Array.isArray(index.categories))
      throw new DirectReferenceExecutionError();
    const categories: DirectFenceSweep['categories'] = [];
    for (const entry of index.categories) {
      const descriptor = object(entry);
      const category = text(descriptor.category);
      const categoryClass = text(descriptor.class);
      const page = (
        await request(
          `/admin/inventory?category=${encodeURIComponent(category)}`,
        )
      ).value;
      if (!Array.isArray(page.entries))
        throw new DirectReferenceExecutionError();
      if (page.cursor !== undefined) text(page.cursor);
      categories.push({
        category,
        class: categoryClass,
        empty: page.entries.length === 0 && page.cursor === undefined,
      });
    }
    return { fence, categories, observedAt: Date.now() };
  }
  if (operation === 'mutate-current') {
    const { value } = await request(DIRECT_TENANT_ROUTES.fenceMutate, {
      phase: 'both',
    });
    return {
      accepted: flag(value.accepted),
      ...(value.code === undefined ? {} : { code: text(value.code) }),
      ...(value.classification === undefined
        ? {}
        : { classification: text(value.classification) }),
      ...(value.status === undefined ? {} : { status: counter(value.status) }),
    };
  }
  if (!isProbeOperation(operation)) throw new DirectReferenceExecutionError();
  const epoch = PROBE_EPOCHS[operation];
  const { value } = await request(DIRECT_TENANT_ROUTES.fenceProbe, { epoch });
  const classification = text(value.classification);
  if (
    value.epoch !== epoch ||
    ![
      'accepted',
      'missing',
      'stale',
      'future',
      'fenced',
      'unexpected',
    ].includes(classification)
  )
    throw new DirectReferenceExecutionError();
  return {
    epoch,
    classification,
    ...(value.status === undefined ? {} : { status: counter(value.status) }),
  };
}
