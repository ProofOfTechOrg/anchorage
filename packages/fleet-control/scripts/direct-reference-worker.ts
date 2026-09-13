// SPDX-License-Identifier: Apache-2.0

import { readBoundedBody } from '@proofoftech/flowsafe/host-kit';
import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import {
  createDirectReferenceContext,
  type DirectReferenceContext,
  type DirectReferenceEnvironment,
} from './direct-reference-context.js';
import type { DirectReferenceAction } from './direct-reference-contract.mjs';
import { dispatchDirectFence } from './direct-reference-fence.js';
import {
  observeDirectForce,
  recoverDirectForce,
  recoverDirectForceResidual,
} from './direct-reference-force.js';
import {
  DirectReferenceExecutionError,
  handleDirectReferenceHttpRequest,
} from './direct-reference-http.js';
import { dispatchDirectInventory } from './direct-reference-inventory.js';
import type { DirectOperationSlot } from './direct-reference-journal.js';
import { dispatchDirectLifecycle } from './direct-reference-lifecycle.js';
import { dispatchDirectR4 } from './direct-reference-r4.js';
import type { DirectReferenceTransportSnapshot } from './direct-reference-transport.js';

const operationSlots: readonly DirectOperationSlot[] = [
  'inventory-before',
  'inventory-after',
  'audit-before',
  'audit-after',
  'migration-next',
  'cleanup-a',
  'cleanup-b',
  'cleanup-recovery',
  'cleanup-recovery-initial',
  'decommission-a',
  'decommission-b',
  'decommission-recovery',
];

type TenantProbeContext = Pick<
  DirectReferenceContext,
  'transport' | 'roleFor' | 'specFor' | 'secrets'
> & {
  readonly control: Pick<DirectReferenceContext['control'], 'getDeployment'>;
};

export async function probeDirectTenant(
  context: TenantProbeContext,
  manifest: DirectRunManifest,
  action: Extract<DirectReferenceAction, { kind: 'tenant-probe' }>,
  invocationSignal: AbortSignal,
) {
  const record = await context.control.getDeployment(
    manifest.names.roles[action.role].tenantTag,
    manifest.environment,
  );
  if (!record || context.roleFor(record) !== action.role)
    throw new DirectReferenceExecutionError();
  const spec = context.specFor(record);
  const token = context.secrets(action.role).application?.APP_PROBE_TOKEN;
  if (!spec.routeHostname || typeof token !== 'string' || !token)
    throw new DirectReferenceExecutionError();
  const { role, operation } = action;
  const url = new URL(
    operation === 'health' ? '/__direct/health' : '/__direct/object',
    `https://${spec.routeHostname}`,
  );
  const method =
    operation === 'object-put'
      ? 'POST'
      : operation === 'object-delete'
        ? 'DELETE'
        : 'GET';
  const cleanup = new AbortController();
  const signal = AbortSignal.any([
    invocationSignal,
    cleanup.signal,
    AbortSignal.timeout(context.transport.effectiveRequestTimeoutMs),
  ]);
  let response: Response | undefined;
  let bodySettled: Promise<void> | undefined;
  try {
    response = await context.transport.applicationFetch(url, {
      method,
      headers: { authorization: `Bearer ${token}` },
      signal,
    });
    const media = response.headers
      .get('content-type')
      ?.split(';')[0]
      ?.trim()
      .toLowerCase();
    if (
      response.status !== (method === 'GET' ? 200 : 204) ||
      (method === 'GET' && media !== 'application/json')
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
      method === 'GET' ? 1024 : 0,
    );
    if (!bounded.ok) throw new DirectReferenceExecutionError();
    if (method !== 'GET') return { role, operation, returned: true };
    let decoded: unknown;
    try {
      decoded = JSON.parse(bounded.text);
    } catch {
      throw new DirectReferenceExecutionError();
    }
    if (!decoded || typeof decoded !== 'object' || Array.isArray(decoded))
      throw new DirectReferenceExecutionError();
    const value = decoded as Record<string, unknown>;
    const fields = Object.keys(value).sort().join(',');
    if (operation === 'health') {
      if (
        fields !== 'marker,release' ||
        (value.release !== '1' && value.release !== '2') ||
        (value.marker !== 'initial' &&
          value.marker !== 'next' &&
          value.marker !== null)
      )
        throw new DirectReferenceExecutionError();
      return { role, operation, release: value.release, marker: value.marker };
    }
    if (fields === 'present' && value.present === false)
      return { role, operation, present: false };
    if (
      fields !== 'present,sha256,size' ||
      value.present !== true ||
      typeof value.size !== 'number' ||
      !Number.isSafeInteger(value.size) ||
      value.size < 1 ||
      typeof value.sha256 !== 'string' ||
      !/^[a-f0-9]{64}$/u.test(value.sha256)
    )
      throw new DirectReferenceExecutionError();
    return {
      role,
      operation,
      present: true,
      size: value.size,
      sha256: value.sha256,
    };
  } finally {
    cleanup.abort();
    await bodySettled;
    if (response && !response.bodyUsed && !response.body?.locked)
      await response.body?.cancel().catch(() => undefined);
  }
}

async function dispatch(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  action: DirectReferenceAction,
  signal: AbortSignal,
): Promise<unknown> {
  if (action.kind === 'control-read') {
    const operations = await Promise.all(
      operationSlots.map((slot) => context.journal.readOperation(slot)),
    );
    const records = await Promise.all(
      (['a', 'b', 'recovery'] as const).map(async (role) => {
        const record = await context.control.getDeployment(
          manifest.names.roles[role].tenantTag,
          manifest.environment,
        );
        if (!record) return { role, present: false };
        context.roleFor(record);
        return {
          role,
          present: true,
          phase: record.phase,
          desiredSpecDigest: record.desiredSpecDigest,
          pendingSpecDigest: record.pendingSpecDigest,
          artifactVersion: record.artifactVersion,
          pendingArtifactVersion: record.pendingArtifactVersion,
          databaseId: record.databaseId,
        };
      }),
    );
    return {
      binding: context.binding,
      operations: operations.filter((value) => value !== undefined),
      records,
      interruption: await context.journal.readInterruption(),
      forceBefore: await context.journal.readForceBefore(),
      forceAfter: await context.journal.readForceAfter(),
    };
  }
  if (action.kind === 'force-recovery')
    return recoverDirectForce(context, manifest);
  if (action.kind === 'force-observe') return observeDirectForce(context);
  if (action.kind === 'recover-force-residual')
    return recoverDirectForceResidual(context);
  if (action.kind === 'tenant-probe')
    return probeDirectTenant(context, manifest, action, signal);
  if (action.kind === 'tenant-fence')
    return dispatchDirectFence(context, manifest, action, signal);
  if ('role' in action)
    return dispatchDirectLifecycle(context, manifest, action, signal);
  if (
    'slot' in action &&
    (action.slot === 'inventory-before' || action.slot === 'inventory-after')
  )
    return dispatchDirectInventory(context, manifest, action, signal);
  return dispatchDirectR4(context, manifest, action, signal);
}

export function createDirectReferenceWorker(
  manifest: DirectRunManifest,
  runtime: Readonly<{ fetch?: typeof fetch }> = {},
) {
  return {
    async fetch(
      request: Request,
      environment: DirectReferenceEnvironment,
    ): Promise<Response> {
      const startedAt = performance.now();
      let metrics: DirectReferenceTransportSnapshot | undefined;
      const response = await handleDirectReferenceHttpRequest(request, {
        invokeSecret: environment.FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET,
        configSha256: manifest.configSha256,
        invocationTimeoutMs: manifest.referenceRuntime.invocationTimeoutMs,
        startedAt,
        dispatch: async (action, signal) => {
          const context = await createDirectReferenceContext(
            manifest,
            environment,
            { startedAt, signal, fetch: runtime.fetch },
          );
          let result: unknown;
          try {
            result = await dispatch(context, manifest, action, signal);
          } finally {
            metrics = context.transport.snapshot();
          }
          context.transport.assertWithinBudget();
          return result;
        },
      });
      if (metrics) {
        response.headers.set(
          'X-Direct-Provider-Attempts',
          String(metrics.providerAttempts),
        );
        response.headers.set(
          'X-Direct-Maintenance-Attempts',
          String(metrics.maintenanceAttempts),
        );
        response.headers.set(
          'X-Direct-Application-Attempts',
          String(metrics.applicationAttempts),
        );
      }
      return response;
    },
  };
}
