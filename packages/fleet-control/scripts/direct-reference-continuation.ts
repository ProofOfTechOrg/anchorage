// SPDX-License-Identifier: Apache-2.0

import { fleetSettlementKey, migrateFleet } from '@proofoftech/fleet-control';
import { deploymentSpecDigest } from '@proofoftech/fleet-control/cloudflare-control-plane';
import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import {
  DIRECT_CONTINUATION_STEP,
  DIRECT_CONTINUATION_WORKFLOW,
} from './direct-credentialed-tenant-object.mjs';
import type { DirectReferenceContext } from './direct-reference-context.js';

export { directContinuation } from './direct-reference-continuation-token.js';

import type { DirectReferenceAction } from './direct-reference-contract.mjs';
import { DirectReferenceExecutionError } from './direct-reference-http.js';
import { readFrozenLifecycleSpec } from './direct-reference-lifecycle.js';
import { directSettlementHost } from './direct-reference-observations.js';
import {
  decodeDirectJsonObject,
  readBoundedDirectResponse,
} from './direct-reference-transport.js';

export async function migrateDirectContinuation(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
) {
  const names = manifest.names.roles.a;
  const replacement = await context.journal.readOperation(
    'cleanup-a-reprovision',
  );
  const initialSpec = context.spec('a', 'initial');
  if (
    replacement?.kind !== 'cleanup' ||
    replacement.tokenJson !== null ||
    readFrozenLifecycleSpec(context, replacement, 'a') !== initialSpec
  )
    throw new DirectReferenceExecutionError();
  const entry = await context.control.getDeployment(
    names.tenantTag,
    manifest.environment,
  );
  if (
    !entry ||
    context.roleFor(entry) !== 'a' ||
    entry.phase !== 'ready' ||
    entry.desiredSpecDigest !== deploymentSpecDigest(initialSpec)
  )
    throw new DirectReferenceExecutionError();
  const databaseId = entry.databaseId;
  const scriptName = entry.scriptName;
  const routeHostname = entry.routeHostname;
  const initialVersionId = entry.artifactVersion;
  const initialSpecDigest = entry.desiredSpecDigest;
  const targetSpec = context.spec('a', 'next');
  const targetSpecDigest = deploymentSpecDigest(targetSpec);
  const plane = context.createForcePlane();
  const validate = (record: typeof entry) => {
    if (
      context.roleFor(record) !== 'a' ||
      record.databaseId !== databaseId ||
      record.scriptName !== scriptName ||
      record.routeHostname !== routeHostname
    )
      throw new DirectReferenceExecutionError();
  };
  const [result] = await migrateFleet({
    store: plane.store,
    records: [entry],
    canaryTenantTags: [],
    backendFor(record) {
      validate(record);
      return plane.backend;
    },
    specFor(record) {
      validate(record);
      return targetSpec;
    },
    secretsFor(record) {
      validate(record);
      return context.secrets('a');
    },
    settlementFor(record) {
      validate(record);
      return directSettlementHost(context, record);
    },
  });
  if (
    !result ||
    context.roleFor(result) !== 'a' ||
    result.phase !== 'ready' ||
    result.databaseId !== databaseId ||
    result.scriptName !== scriptName ||
    result.routeHostname !== routeHostname ||
    result.desiredSpecDigest !== targetSpecDigest ||
    !initialVersionId ||
    !result.artifactVersion ||
    initialVersionId === result.artifactVersion
  )
    throw new DirectReferenceExecutionError();
  return {
    databaseId,
    scriptName,
    routeHostname,
    initialVersionId,
    finalVersionId: result.artifactVersion,
    initialSpecDigest,
    targetSpecDigest,
    settlementKey: fleetSettlementKey({
      tenantTag: result.tenantTag,
      environment: result.environment,
      specDigest: targetSpecDigest,
      artifactVersion: result.artifactVersion,
    }),
  };
}

export async function dispatchDirectContinuation(
  context: DirectReferenceContext,
  manifest: DirectRunManifest,
  action: Extract<DirectReferenceAction, { kind: 'tenant-continuation' }>,
  invocationSignal: AbortSignal,
) {
  const record = await context.control.getDeployment(
    manifest.names.roles.a.tenantTag,
    manifest.environment,
  );
  if (!record || context.roleFor(record) !== 'a')
    throw new DirectReferenceExecutionError();
  const spec = context.specFor(record);
  const token = context.secrets('a').application?.APP_PROBE_TOKEN;
  if (!spec.routeHostname || typeof token !== 'string' || !token)
    throw new DirectReferenceExecutionError();
  const start = action.operation === 'start';
  const runId = start ? undefined : action.runId;
  const resume =
    action.operation === 'resume-locked' || action.operation === 'resume';
  const path = start
    ? '/runs'
    : `/runs/${DIRECT_CONTINUATION_WORKFLOW}/${runId}${resume ? '/resume' : ''}`;
  const method = start || resume ? 'POST' : 'GET';
  const { status, text } = await readBoundedDirectResponse({
    fetch: context.transport.applicationFetch,
    url: new URL(path, `https://${spec.routeHostname}`),
    method,
    token,
    ...(start
      ? {
          body: {
            workflowId: DIRECT_CONTINUATION_WORKFLOW,
            inputData: { challenge: action.challenge },
          },
        }
      : resume
        ? {
            body: {
              step: DIRECT_CONTINUATION_STEP,
              resumeData: { proceed: true },
            },
          }
        : {}),
    acceptStatuses: action.operation === 'resume-locked' ? [503] : [200],
    mediaType: 'application/json',
    byteLimit: 64 * 1024,
    invocationSignal,
    requestTimeoutMs: context.transport.effectiveRequestTimeoutMs,
  });
  const value = decodeDirectJsonObject(text);
  if (action.operation === 'resume-locked') {
    const reason = value.reason;
    if (
      !reason ||
      typeof reason !== 'object' ||
      Array.isArray(reason) ||
      Reflect.get(reason, 'code') !== 'EXECUTION_FENCED' ||
      Reflect.get(reason, 'state') !== 'migration-locked'
    )
      throw new DirectReferenceExecutionError();
    return { runId, status, reason };
  }
  if (action.operation === 'resume') {
    const decision = await readBoundedDirectResponse({
      fetch: context.transport.applicationFetch,
      url: new URL(
        `/api/approvals/${action.approvalId}/decide`,
        `https://${spec.routeHostname}`,
      ),
      method: 'POST',
      token,
      body: { decision: 'approve' },
      acceptStatuses: [200],
      mediaType: 'application/json',
      byteLimit: 64 * 1024,
      invocationSignal,
      requestTimeoutMs: context.transport.effectiveRequestTimeoutMs,
    });
    const decisionValue = decodeDirectJsonObject(decision.text);
    const approval = decisionValue.record;
    if (
      !approval ||
      typeof approval !== 'object' ||
      Array.isArray(approval) ||
      Reflect.get(approval, 'id') !== action.approvalId ||
      Reflect.get(approval, 'runId') !== runId ||
      Reflect.get(approval, 'status') !== 'approved'
    )
      throw new DirectReferenceExecutionError();
    return {
      status,
      summary: value,
      approval: { id: action.approvalId, status: 'approved' },
    };
  }
  return { status, summary: value };
}
