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

export function recoveryApplicationSpec(
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
  const recoverySpecs = new Map<
    CloudflareDeploymentSpec,
    CloudflareDeploymentSpec
  >();
  const baseDigestByRecoveryDigest = new Map<string, string>();
  for (const role of ['a', 'b', 'recovery'] as const) {
    for (const release of [
      'initial',
      'next',
      ...(role === 'recovery' ? ['failed-recovery' as const] : []),
    ] as const) {
      const spec = context.spec(role, release);
      const replacement =
        role === 'recovery' ? recoveryApplicationSpec(spec, bindingName) : spec;
      specs.set(`${role}:${release}`, replacement);
      if (replacement !== spec) {
        recoverySpecs.set(spec, replacement);
        baseDigestByRecoveryDigest.set(
          deploymentSpecDigest(replacement),
          deploymentSpecDigest(spec),
        );
      }
    }
  }
  return {
    ...context,
    spec(role: DirectFixtureRole, release: DirectFixtureRelease) {
      return specs.get(`${role}:${release}`) ?? context.spec(role, release);
    },
    specFor(record: FleetRecord) {
      const normalized = JSON.parse(
        JSON.stringify(record, (_key, value) =>
          typeof value === 'string'
            ? (baseDigestByRecoveryDigest.get(value) ?? value)
            : value,
        ),
      ) as FleetRecord;
      const selected = context.specFor(normalized);
      return recoverySpecs.get(selected) ?? selected;
    },
  };
}
