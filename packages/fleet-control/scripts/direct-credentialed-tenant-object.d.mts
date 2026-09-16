// SPDX-License-Identifier: Apache-2.0

export const DIRECT_TENANT_OBJECT_KEY: 'direct-conformance-fixture';
export const DIRECT_TENANT_OBJECT_BODY: 'direct-conformance-fixture-data';
export function directTenantMutationEpoch(release: string | undefined): number;
export function directTenantProbeEpoch(
  release: string | undefined,
  label: unknown,
): number | undefined;
