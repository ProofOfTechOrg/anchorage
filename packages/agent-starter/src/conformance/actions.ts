// SPDX-License-Identifier: Apache-2.0

import { createConnector, invokeConnector } from '@proofoftech/breakwater';
import {
  createAuditProxyDurableObjectBinding,
  createAuditProxyQueue,
} from '@proofoftech/flowsafe/audit-export';
import { deploymentIdentityHeaders } from '@proofoftech/flowsafe/deployment-identity-protocol';
import { z } from 'zod';

import {
  CONFORMANCE_CONTRACT,
  CONFORMANCE_CONTRACT_VERSION,
  requiredText,
} from './contract.js';
import type { ConformanceCandidateEnv, NamespaceLike } from './env.js';

/**
 * The candidate's half of the artifact contract. Every response body is built
 * by `contractResponse`, which is the only place `contractVersion` and `action`
 * are added — the gate compares the response's key set EXACTLY, so one extra
 * field anywhere fails the run.
 */

/** Bounded work sized well under the 50 ms dispatch limit. */
const CPU_CONTROL_ITERATIONS = 200_000;

export function contractResponse(
  action: string,
  fields: Record<string, unknown>,
  status = 200,
): Response {
  return Response.json(
    { contractVersion: CONFORMANCE_CONTRACT_VERSION, action, ...fields },
    { status },
  );
}

function hex(bytes: ArrayBuffer): string {
  return Array.from(new Uint8Array(bytes), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
}

async function hmacSha256Hex(secret: string, message: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  return hex(
    await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(message)),
  );
}

function stub(namespace: NamespaceLike, instanceName: string) {
  return namespace.get(namespace.idFromName(instanceName));
}

/**
 * Forward one action to the trusted state script and answer with whatever it
 * decided, including its status. The state object owns the FlowSafe lifecycle
 * because the run must survive this candidate being replaced.
 */
export async function throughState(
  env: ConformanceCandidateEnv,
  action: string,
  payload: Record<string, unknown>,
): Promise<Response> {
  const response = await stub(
    env.CONFORMANCE_STATE,
    CONFORMANCE_CONTRACT.stateInstanceName,
  ).fetch(
    new Request('http://conformance-state/internal/action', {
      method: 'POST',
      headers: deploymentIdentityHeaders(env.DEPLOYMENT_IDENTITY_SECRET, {
        'content-type': 'application/json',
      }),
      body: JSON.stringify({ action, ...payload }),
    }),
  );
  const fields = (await response.json()) as Record<string, unknown>;
  if (response.status !== 200 && response.status !== 409) {
    throw new Error(
      `trusted state rejected '${action}': ${JSON.stringify(fields)}`,
    );
  }
  return contractResponse(action, fields, response.status);
}

async function applicationBindings(
  env: ConformanceCandidateEnv,
  nonce: string,
): Promise<Response> {
  return contractResponse('application-bindings', {
    variableName: CONFORMANCE_CONTRACT.applicationVariableName,
    variableValue: env.APPLICATION_MODE,
    secretName: CONFORMANCE_CONTRACT.applicationSecretBinding,
    secretHmacSha256: await hmacSha256Hex(
      env.APPLICATION_CONFORMANCE_SECRET,
      nonce,
    ),
    // Proving possession by HMAC rather than by echo is the point of the
    // action: the control plane keeps only a SHA-256 of this value.
    secretPlaintextExposed: false,
  });
}

async function auditProxy(
  env: ConformanceCandidateEnv,
  nonce: string,
): Promise<Response> {
  const queue = createAuditProxyQueue<Record<string, unknown>>(
    createAuditProxyDurableObjectBinding(env.AUDIT_PROXY),
    env.DEPLOYMENT_IDENTITY_SECRET,
  );
  // The trusted proxy discards caller-selected attribution and re-wraps this in
  // its own envelope; action/resource/decision are the fields it validates.
  await queue.send({
    action: 'conformance.audit-proxy',
    resource: `conformance:${nonce}`,
    decision: 'allowed',
    nonce,
  });
  return contractResponse('audit-proxy', { nonce, accepted: true });
}

/**
 * The egress probe declares the host the gate asked for, on purpose.
 *
 * DO NOT COPY THIS INTO A REAL CONNECTOR. A production manifest names its
 * upstreams statically so Breakwater can deny an undeclared host before any
 * request leaves. Here the boundary under test is the Workers for Platforms
 * outbound Worker, one layer further out, and the gate requires an ACTUAL
 * upstream status for the denied URL. Declaring the requested host makes
 * Breakwater's declaration gate transparent so the platform's decision is what
 * the response reports.
 */
function createEgressProbeConnector(hostname: string) {
  return createConnector<{ url: string }, { upstreamStatus: number }>({
    id: 'conformance_egressProbe',
    description: 'Probe one upstream through the connector egress guard',
    inputSchema: z.object({ url: z.string().url() }),
    outputSchema: z.object({ upstreamStatus: z.number() }),
    permissions: { sideEffect: 'read', egress: [hostname] },
    execute: async ({ url }, _context, runtime) => {
      const response = await runtime.fetch(url, { redirect: 'manual' });
      return { upstreamStatus: response.status };
    },
  });
}

async function connectorEgress(
  action: 'connector-egress-allowed' | 'connector-egress-denied',
  url: string,
): Promise<Response> {
  const probe = createEgressProbeConnector(new URL(url).hostname);
  const { upstreamStatus } = await invokeConnector(probe, { url });
  return contractResponse(
    action,
    action === 'connector-egress-allowed'
      ? { allowed: true, upstreamStatus }
      : { denied: true, upstreamStatus },
  );
}

function burnCpu(iterations: number): number {
  let value = 1;
  for (let index = 0; index < iterations; index += 1) {
    value = (value * 31 + index) % 2_147_483_647;
  }
  return value;
}

function cpuControl(): Response {
  // The loop's result is folded into the response as a boolean rather than
  // dropped, so a bundler cannot eliminate the work this action exists to do.
  // The key set stays exact because only the boolean escapes.
  const burned = burnCpu(CPU_CONTROL_ITERATIONS);
  return contractResponse('cpu-control', { completed: burned >= 0 });
}

/**
 * Deliberately unbounded. Cloudflare must terminate the request with the
 * configured `cpuOverLimitStatus`; the gate never parses a body here, so any
 * response this function could return would fail the action.
 */
function cpuOverLimit(): never {
  // burnCpu is never negative, so the guard never fires; it is there so the
  // loop reads as terminating code and cannot be optimized away.
  let burned = 0;
  while (burned >= 0) {
    burned = burnCpu(1_000_000);
  }
  throw new Error('unreachable: the platform must terminate this request');
}

async function r2Action(
  env: ConformanceCandidateEnv,
  action: 'r2-write' | 'r2-read' | 'r2-delete' | 'r2-absent',
  key: string,
  value?: string,
): Promise<Response> {
  const bucket = env.APPLICATION_FILES;
  if (action === 'r2-write') {
    if (value === undefined) throw new Error('value is required');
    await bucket.put(key, value);
    return contractResponse(action, { key, written: true });
  }
  if (action === 'r2-read') {
    const object = await bucket.get(key);
    if (!object) throw new Error(`R2 object '${key}' is absent`);
    return contractResponse(action, { key, value: await object.text() });
  }
  if (action === 'r2-delete') {
    await bucket.delete(key);
    return contractResponse(action, { key, deleted: true });
  }
  // A provider read, not a cached negative: `head` is what proves absence.
  return contractResponse(action, {
    key,
    absent: (await bucket.head(key)) === null,
  });
}

async function stateNewClass(
  env: ConformanceCandidateEnv,
  nonce: string,
): Promise<Response> {
  const namespace = env.CONFORMANCE_V2;
  if (!namespace) {
    // Release one has no such binding. Answering in contract shape rather than
    // throwing keeps the failure legible if the gate ever asks too early.
    return contractResponse('state-new-class', { nonce, stored: false }, 409);
  }
  const response = await stub(
    namespace,
    CONFORMANCE_CONTRACT.v2InstanceName,
  ).fetch(
    new Request('http://conformance-v2/internal/store', {
      method: 'POST',
      headers: deploymentIdentityHeaders(env.DEPLOYMENT_IDENTITY_SECRET, {
        'content-type': 'application/json',
      }),
      body: JSON.stringify({ nonce }),
    }),
  );
  const fields = (await response.json()) as Record<string, unknown>;
  if (!response.ok) {
    throw new Error(`state-new-class failed: ${JSON.stringify(fields)}`);
  }
  return contractResponse('state-new-class', fields);
}

export interface ConformanceRequest {
  readonly contractVersion?: unknown;
  readonly action: string;
  readonly nonce?: unknown;
  readonly url?: unknown;
  readonly key?: unknown;
  readonly value?: unknown;
  readonly marker?: unknown;
  readonly effectNonce?: unknown;
  readonly runId?: unknown;
  readonly approvalId?: unknown;
  readonly revision?: unknown;
}

export async function handleConformanceAction(
  env: ConformanceCandidateEnv,
  body: ConformanceRequest,
): Promise<Response> {
  switch (body.action) {
    case 'application-bindings':
      return applicationBindings(env, requiredText(body.nonce, 'nonce'));
    case 'audit-proxy':
      return auditProxy(env, requiredText(body.nonce, 'nonce'));
    case 'connector-egress-allowed':
    case 'connector-egress-denied':
      return connectorEgress(body.action, requiredText(body.url, 'url'));
    case 'cpu-control':
      return cpuControl();
    case 'cpu-over-limit':
      return cpuOverLimit();
    case 'r2-write':
      return r2Action(
        env,
        body.action,
        requiredText(body.key, 'key'),
        requiredText(body.value, 'value'),
      );
    case 'r2-read':
    case 'r2-delete':
    case 'r2-absent':
      return r2Action(env, body.action, requiredText(body.key, 'key'));
    case 'state-new-class':
      return stateNewClass(env, requiredText(body.nonce, 'nonce'));
    case 'state-marker-put':
    case 'state-marker-get':
      return throughState(env, body.action, {
        marker: requiredText(body.marker, 'marker'),
      });
    case 'state-egress-allowed':
    case 'state-egress-denied':
      return throughState(env, body.action, {
        url: requiredText(body.url, 'url'),
      });
    case 'flowsafe-start':
      return throughState(env, body.action, {
        effectNonce: requiredText(body.effectNonce, 'effectNonce'),
      });
    case 'flowsafe-status':
      return throughState(env, body.action, {
        runId: requiredText(body.runId, 'runId'),
      });
    case 'flowsafe-approve':
    case 'flowsafe-replay-decision':
    case 'flowsafe-replay-resume':
      return throughState(env, body.action, {
        runId: requiredText(body.runId, 'runId'),
        approvalId: requiredText(body.approvalId, 'approvalId'),
        revision: Number(body.revision),
      });
    default:
      throw new Error(`unknown conformance action '${body.action}'`);
  }
}
