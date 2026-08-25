// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import {
  type AttestConvergedActiveRouteOptions,
  attestConvergedActiveRoute,
  PENDING_ARTIFACT_VERSION,
} from './active-route.js';
import type {
  ActiveRouteAttestation,
  DeploymentSpec,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetSettlementEntry,
  FleetSettlementHost,
  ProvisioningBackend,
} from './types.js';

/**
 * Identifies a settlement by the deployment and the release that settled.
 *
 * Deliberately excludes the entry, the attempt, and the clock. A migration that
 * crashes after `settle()` and re-enters must produce the same key or the
 * at-least-once contract is worthless, and a rollback that returns a
 * deployment to a release it settled before is settling that same release
 * again — the same key is the honest answer, not a collision.
 */
export function fleetSettlementKey(input: {
  readonly tenantTag: string;
  readonly environment: string;
  readonly specDigest: string;
  readonly artifactVersion: string;
}): string {
  return createHash('sha256')
    .update(
      JSON.stringify({
        tenantTag: input.tenantTag,
        environment: input.environment,
        specDigest: input.specDigest,
        artifactVersion: input.artifactVersion,
      }),
    )
    .digest('hex');
}

/**
 * Resolves the snapshot a settlement reports as its target.
 *
 * Two adjustments, both narrow. A planned release still carrying the `pending`
 * sentinel takes the attested version, because a key built from `pending` would
 * differ on every retry and defeat deduplication.
 *
 * A deployment with no release snapshots at all — the plain backend keeps
 * none — gets one synthesized, and it is a MIXED record rather than a provider
 * reading. `physicalScriptName`, `specDigest`, and `artifactVersion` are the
 * attestation's, so the three fields the settlement key is built from are
 * provider truth. `releaseSchemaVersion` and `application` are copied from the
 * control-plane record, because no provider read reports them; they are what
 * this deployment believes it deployed, and a host must not treat them as
 * attested.
 */
function settledTargetRelease(
  target: ExternalReleaseSnapshot | undefined,
  attestation: ActiveRouteAttestation,
  record: FleetRecord,
): ExternalReleaseSnapshot {
  if (!target) {
    return {
      physicalScriptName: attestation.physicalScriptName,
      specDigest: attestation.specDigest,
      artifactVersion: attestation.artifactVersion,
      releaseSchemaVersion: record.schemaVersion,
      ...(record.applicationBindings
        ? { application: record.applicationBindings }
        : {}),
    };
  }
  return target.artifactVersion === PENDING_ARTIFACT_VERSION
    ? { ...target, artifactVersion: attestation.artifactVersion }
    : target;
}

export interface SettlePromotedRouteInput {
  readonly backend: ProvisioningBackend;
  readonly spec: DeploymentSpec;
  /** The record as it stands after the promotion, before the settling write. */
  readonly record: FleetRecord;
  readonly entry: FleetSettlementEntry;
  /** The release expected to be serving; absent where none is persisted. */
  readonly target: ExternalReleaseSnapshot | undefined;
  readonly prior: ExternalReleaseSnapshot | undefined;
  /** The digest the route must be serving. Never taken from the provider. */
  readonly expectedSpecDigest: string;
  /** The artifact expected, or `pending` when none is known yet. */
  readonly expectedArtifactVersion: string;
  readonly settlementHost: FleetSettlementHost | undefined;
  readonly attestation: AttestConvergedActiveRouteOptions | undefined;
  /**
   * Set on the convergence entry only. That entry is re-entered on every
   * routine reconcile of an unchanged deployment, so re-firing settlement there
   * would bill a fleet for standing still. Every other entry follows a change
   * and settles unconditionally, which is what keeps at-least-once honest
   * across a crash between `settle()` and the write that records it.
   */
  readonly skipWhenAlreadySettled?: true;
}

export interface SettledPromotedRoute {
  readonly attestation: ActiveRouteAttestation;
  readonly settlementKey: string;
  /** Whether `settle()` was actually called on this pass. */
  readonly settled: boolean;
}

/**
 * Attest what the promotion published, then settle it — in that order, and only
 * in that order.
 *
 * Attestation is unconditional and runs whether or not a host settles: proving
 * the promotion took effect is the package's own obligation, and gating it on
 * an optional callback would mean a fleet without one never checks its own
 * work. Settlement is what is optional, and it runs only once attestation has
 * both succeeded and matched, so a host is never told a release is live on the
 * strength of a promotion this process merely issued.
 */
export async function settlePromotedRoute(
  input: SettlePromotedRouteInput,
): Promise<SettledPromotedRoute> {
  const attestation = await attestConvergedActiveRoute(
    input.backend,
    input.spec,
    {
      specDigest: input.expectedSpecDigest,
      artifactVersion: input.expectedArtifactVersion,
    },
    input.attestation ?? {},
  );
  const target = settledTargetRelease(input.target, attestation, input.record);
  const settlementKey = fleetSettlementKey({
    tenantTag: input.record.tenantTag,
    environment: input.record.environment,
    specDigest: target.specDigest,
    artifactVersion: target.artifactVersion,
  });
  const alreadySettled = input.record.settledSettlementKey === settlementKey;
  if (
    !input.settlementHost ||
    (input.skipWhenAlreadySettled && alreadySettled)
  ) {
    return { attestation, settlementKey, settled: false };
  }
  await input.settlementHost.settle({
    tenantTag: input.record.tenantTag,
    environment: input.record.environment,
    attestation,
    target,
    ...(input.prior ? { prior: input.prior } : {}),
    entry: input.entry,
    settlementKey,
    alreadySettled,
  });
  return { attestation, settlementKey, settled: true };
}
