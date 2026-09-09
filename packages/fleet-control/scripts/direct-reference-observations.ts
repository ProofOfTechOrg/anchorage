// SPDX-License-Identifier: Apache-2.0

import { fleetSettlementKey } from '@proofoftech/fleet-control';
import {
  deploymentSpecDigest,
  type FleetRecord,
  type FleetSettlementHost,
} from '@proofoftech/fleet-control/cloudflare-control-plane';
import type { DirectFixtureRelease } from './direct-credentialed-spec.js';
import type { DirectReferenceContext } from './direct-reference-context.js';
import { DirectReferenceExecutionError } from './direct-reference-http.js';
import type { DirectStoredResource } from './direct-reference-journal.js';

type ResourceSource = 'provision-read' | 'migration-read' | 'before-force';

function compare(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export async function recordDirectResource(
  context: DirectReferenceContext,
  record: FleetRecord,
  source: ResourceSource,
): Promise<DirectStoredResource> {
  context.transport.assertWithinBudget();
  const role = context.roleFor(record);
  context.specFor(record);
  if (
    record.durableObjectBindings.some(
      (binding) => binding.scriptName || binding.dispatchNamespace,
    )
  )
    throw new DirectReferenceExecutionError();
  const admittedPhase =
    record.cleanupIntent?.identity.admittedPhase ?? record.phase;
  const databaseState =
    admittedPhase === 'database-reserved'
      ? 'reservation'
      : admittedPhase === 'database-create-authorized'
        ? 'create-outcome-unresolved'
        : 'recorded';
  const identity = {
    version: 1,
    role,
    backend: record.backend,
    tenantTag: record.tenantTag,
    environment: record.environment,
    scriptName: record.scriptName,
    database: {
      name: record.databaseName,
      id: record.databaseId.startsWith('reserved-') ? null : record.databaseId,
    },
    knownVersionIds: [
      ...new Set([record.artifactVersion, record.pendingArtifactVersion]),
    ]
      .filter(
        (value): value is string => value !== undefined && value !== 'pending',
      )
      .sort(compare),
    localNamespaces: record.durableObjectBindings
      .map(({ name, className, namespaceId }) => ({
        name,
        className,
        namespaceId,
      }))
      .sort((left, right) => compare(left.name, right.name)),
    applicationBuckets: (record.applicationResources ?? [])
      .map(
        ({
          name,
          bucketName,
          jurisdiction,
          reservationNonce,
          creationDate,
        }) => ({
          name,
          bucketName,
          jurisdiction,
          reservationNonce,
          creationDate: creationDate ?? null,
        }),
      )
      .sort((left, right) => compare(left.name, right.name)),
  };
  const stored = await context.journal.recordResource(
    role,
    JSON.stringify(identity),
    JSON.stringify({
      source,
      phase: record.phase,
      schemaVersion: record.schemaVersion,
      desiredSpecDigest: record.desiredSpecDigest,
      pendingSpecDigest: record.pendingSpecDigest ?? null,
      recordUpdatedAt: record.updatedAt,
      databaseState,
      applicationStates: (record.applicationResources ?? [])
        .map(({ name, state }) => ({ name, state }))
        .sort((left, right) => compare(left.name, right.name)),
    }),
  );
  context.transport.assertWithinBudget();
  return stored;
}

export function directSettlementHost(
  context: DirectReferenceContext,
  record: FleetRecord,
): FleetSettlementHost {
  const role = context.roleFor(record);
  context.specFor(record);
  const releases: readonly DirectFixtureRelease[] =
    role === 'recovery'
      ? ['initial', 'next', 'failed-recovery']
      : ['initial', 'next'];
  const digests = new Set(
    releases.map((release) =>
      deploymentSpecDigest(context.spec(role, release)),
    ),
  );
  return {
    async settle(settlement) {
      context.transport.assertWithinBudget();
      const { target, attestation, settlementKey } = settlement;
      if (
        settlement.tenantTag !== record.tenantTag ||
        settlement.environment !== record.environment ||
        target.physicalScriptName !== record.scriptName ||
        !digests.has(target.specDigest) ||
        !target.artifactVersion ||
        target.artifactVersion === 'pending' ||
        attestation.physicalScriptName !== target.physicalScriptName ||
        attestation.specDigest !== target.specDigest ||
        attestation.artifactVersion !== target.artifactVersion ||
        settlementKey !==
          fleetSettlementKey({
            tenantTag: record.tenantTag,
            environment: record.environment,
            specDigest: target.specDigest,
            artifactVersion: target.artifactVersion,
          })
      )
        throw new DirectReferenceExecutionError();
      await context.journal.recordSettlement(
        settlementKey,
        JSON.stringify({
          version: 1,
          role,
          tenantTag: record.tenantTag,
          environment: record.environment,
          target: {
            physicalScriptName: target.physicalScriptName,
            specDigest: target.specDigest,
            artifactVersion: target.artifactVersion,
          },
        }),
        JSON.stringify({
          entry: settlement.entry,
          alreadySettled: settlement.alreadySettled,
          observedAt: attestation.observedAt,
        }),
      );
      context.transport.assertWithinBudget();
    },
  };
}
