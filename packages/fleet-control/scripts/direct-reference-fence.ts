// SPDX-License-Identifier: Apache-2.0

import type { ExecutionFenceState as FenceState } from '@proofoftech/flowsafe/do-runner';
import { readBoundedBody } from '@proofoftech/flowsafe/host-kit';
import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectReferenceContext } from './direct-reference-context.js';
import type { DirectReferenceAction } from './direct-reference-contract.mjs';
import { DirectReferenceExecutionError } from './direct-reference-http.js';

type Reading = {
  state: FenceState;
  mutationEpoch: number;
  requireMutationEpoch: boolean;
  transitionRevision: number;
};

type FenceTransitionResult =
  | { ok: true; after: Reading }
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

function reading(value: Record<string, unknown>): Reading {
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
    operation === 'mutate-current' || operation.startsWith('probe-');
  const secrets = context.secrets(action.role);
  const token = application
    ? secrets.application?.APP_PROBE_TOKEN
    : secrets.maintenanceAdmin;
  if (typeof token !== 'string' || !token)
    throw new DirectReferenceExecutionError();

  async function request(path: string, body?: unknown) {
    const url = new URL(path, `https://${spec.routeHostname}`);
    const cleanup = new AbortController();
    const signal = AbortSignal.any([
      invocationSignal,
      cleanup.signal,
      AbortSignal.timeout(context.transport.effectiveRequestTimeoutMs),
    ]);
    let response: Response | undefined;
    let bodySettled: Promise<void> | undefined;
    try {
      const fetch = application
        ? context.transport.applicationFetch
        : context.transport.maintenanceFetch;
      response = await fetch(url, {
        method: body === undefined ? 'GET' : 'POST',
        headers: {
          authorization: `Bearer ${token}`,
          ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal,
      });
      const media = response.headers
        .get('content-type')
        ?.split(';')[0]
        ?.trim()
        .toLowerCase();
      if (
        (response.status !== 200 &&
          !(
            response.status === 409 &&
            (operation === 'drain' || operation === 'reopen')
          )) ||
        media !== 'application/json'
      )
        throw new DirectReferenceExecutionError();
      const stream = new TransformStream<Uint8Array, Uint8Array>();
      bodySettled = response.body
        ?.pipeTo(stream.writable, { signal })
        .catch(() => undefined);
      const bodyInit = {
        method: 'POST',
        headers: response.headers,
        body: response.body ? stream.readable : undefined,
        signal,
        duplex: 'half' as const,
      };
      const bounded = await readBoundedBody(
        new Request(url, bodyInit),
        operation === 'inventory' ? 65536 : 4096,
      );
      if (!bounded.ok) throw new DirectReferenceExecutionError();
      let decoded: unknown;
      try {
        decoded = JSON.parse(bounded.text);
      } catch {
        throw new DirectReferenceExecutionError();
      }
      return { status: response.status, value: object(decoded) };
    } finally {
      cleanup.abort();
      await bodySettled;
      if (response && !response.bodyUsed && !response.body?.locked)
        await response.body?.cancel().catch(() => undefined);
    }
  }

  if (operation === 'read')
    return reading((await request('/admin/execution-fence')).value);
  if (operation === 'drain' || operation === 'reopen') {
    const { expectedMutationEpoch, expectedRevision } = action;
    const result = await request(
      '/admin/execution-fence',
      operation === 'drain'
        ? {
            expected: 'open',
            next: 'draining',
            expectedMutationEpoch,
            expectedRevision,
            advanceMutationEpoch: true,
          }
        : {
            expected: 'draining',
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
    const categories = [];
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
    const { value } = await request('/__direct/fence-mutate', {
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
  const epoch = operation.slice('probe-'.length);
  const { value } = await request('/__direct/fence-probe', { epoch });
  if (
    value.epoch !== epoch ||
    ![
      'accepted',
      'missing',
      'stale',
      'future',
      'fenced',
      'unexpected',
    ].includes(text(value.classification))
  )
    throw new DirectReferenceExecutionError();
  return {
    epoch,
    classification: value.classification,
    ...(value.status === undefined ? {} : { status: counter(value.status) }),
  };
}
