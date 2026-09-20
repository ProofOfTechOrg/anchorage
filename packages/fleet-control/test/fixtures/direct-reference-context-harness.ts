// SPDX-License-Identifier: Apache-2.0

import type {
  DirectFixtureRelease,
  DirectFixtureRole,
} from '../../scripts/direct-credentialed-spec.js';
import {
  createDirectReferenceContext as createBaseDirectReferenceContext,
  type DirectReferenceEnvironment,
} from '../../scripts/direct-reference-context.js';
import type { CloudflareDeploymentSpec } from '../../src/cloudflare-control-plane.js';
import { deploymentSpecDigest } from '../../src/spec-digest.js';
import type { FleetRecord } from '../../src/types.js';

export * from '../../scripts/direct-reference-context.js';

function recoverySpec(
  spec: CloudflareDeploymentSpec,
  bindingName: string | undefined,
): CloudflareDeploymentSpec {
  if (!bindingName) return spec;
  if (!spec.application)
    throw new Error('recovery fixture has no application bindings');
  return {
    ...spec,
    application: {
      ...spec.application,
      r2Buckets: [...spec.application.r2Buckets, { name: bindingName }],
    },
  };
}

function recordSpecDigest(record: FleetRecord): string | undefined {
  let digest =
    record.phase === 'migrating'
      ? record.pendingSpecDigest
      : record.desiredSpecDigest;
  if (record.cleanupIntent?.authority.kind === 'provisioning-rollback')
    digest = record.cleanupIntent.authority.requestedSpecDigest;
  if (record.decommissionIntent?.identity.mode.kind === 'normal')
    digest = record.decommissionIntent.identity.mode.requestedSpecDigest;
  return digest;
}

export async function createDirectReferenceContext(
  ...args: Parameters<typeof createBaseDirectReferenceContext>
): ReturnType<typeof createBaseDirectReferenceContext> {
  const context = await createBaseDirectReferenceContext(...args);
  const environment = args[1] as DirectReferenceEnvironment & {
    DIRECT_RECOVERY_R2_BUCKET?: string;
  };
  const bindingName = environment.DIRECT_RECOVERY_R2_BUCKET;
  if (!bindingName) return context;
  const specs = new Map<string, CloudflareDeploymentSpec>();
  for (const role of ['a', 'b', 'recovery'] as const) {
    for (const release of [
      'initial',
      'next',
      ...(role === 'recovery' ? ['failed-recovery' as const] : []),
    ] as const) {
      const spec = context.spec(role, release);
      specs.set(
        `${role}:${release}`,
        role === 'recovery' ? recoverySpec(spec, bindingName) : spec,
      );
    }
  }
  return {
    ...context,
    spec(role: DirectFixtureRole, release: DirectFixtureRelease) {
      return specs.get(`${role}:${release}`) ?? context.spec(role, release);
    },
    specFor(record: FleetRecord) {
      const role = context.roleFor(record);
      const digest = recordSpecDigest(record);
      const match = [...specs.entries()].find(
        ([key, spec]) =>
          key.startsWith(`${role}:`) && deploymentSpecDigest(spec) === digest,
      );
      return match?.[1] ?? context.specFor(record);
    },
  };
}
