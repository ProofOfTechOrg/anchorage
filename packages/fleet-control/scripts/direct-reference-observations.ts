// SPDX-License-Identifier: Apache-2.0

import {
  fleetSettlementKey,
  type R2Jurisdiction,
} from '@proofoftech/fleet-control';
import {
  deploymentSpecDigest,
  type FleetRecord,
  type FleetSettlementHost,
} from '@proofoftech/fleet-control/cloudflare-control-plane';
import type {
  DirectFixtureRelease,
  DirectFixtureRole,
} from './direct-credentialed-spec.js';
import type { DirectReferenceContext } from './direct-reference-context.js';
import { DirectReferenceExecutionError } from './direct-reference-http.js';
import type { DirectStoredResource } from './direct-reference-journal.js';

type ResourceSource =
  | 'provision-read'
  | 'migration-read'
  | 'teardown-read'
  | 'before-force';

export interface DirectResourceIdentity {
  readonly version: 1;
  readonly role: DirectFixtureRole;
  readonly backend: FleetRecord['backend'];
  readonly tenantTag: string;
  readonly environment: string;
  readonly scriptName: string;
  readonly database: Readonly<{ name: string; id: string | null }>;
  readonly knownVersionIds: readonly string[];
  readonly localNamespaces: readonly Readonly<{
    name: string;
    className: string;
    namespaceId: string;
  }>[];
  readonly applicationBuckets: readonly Readonly<{
    name: string;
    bucketName: string;
    jurisdiction: R2Jurisdiction;
    reservationNonce: string;
    creationDate: string | null;
  }>[];
}

function compare(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export function directResourceObservation(
  context: DirectReferenceContext,
  record: FleetRecord,
  source: ResourceSource,
): Readonly<{
  role: DirectFixtureRole;
  identityJson: string;
  provenanceJson: string;
}> {
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
  const identity: DirectResourceIdentity = {
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
      ...new Set([
        record.artifactVersion,
        record.pendingArtifactVersion,
        record.activeRelease?.artifactVersion,
        record.pendingRelease?.artifactVersion,
      ]),
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
  return {
    role,
    identityJson: JSON.stringify(identity),
    provenanceJson: JSON.stringify({
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
  };
}

export async function recordDirectResource(
  context: DirectReferenceContext,
  record: FleetRecord,
  source: ResourceSource,
): Promise<DirectStoredResource> {
  context.transport.assertWithinBudget();
  const observation = directResourceObservation(context, record, source);
  const stored = await context.journal.recordResource(
    observation.role,
    observation.identityJson,
    observation.provenanceJson,
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
