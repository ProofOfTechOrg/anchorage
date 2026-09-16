// SPDX-License-Identifier: Apache-2.0

export const DIRECT_TENANT_OBJECT_KEY = 'direct-conformance-fixture';
export const DIRECT_TENANT_OBJECT_BODY = 'direct-conformance-fixture-data';

// The caller epoch an artifact of this release carries. Release 2 is the
// post-cutover artifact; release 1 predates the activation and is therefore
// stale once the control plane advances the fence. One definition, because the
// conformance comparison is between the tenant's configured value and the
// fixture's re-derivation of it, and two copies can drift while both lanes pass.
export const directTenantMutationEpoch = (release) => (release === '2' ? 1 : 0);

// The epoch a caller selects for a fence probe, positioned against the host
// epoch above. One definition for the same reason: the tenant Worker and the
// reference fixture answer the same labels from the same release.
export const directTenantProbeEpoch = (release, label) => {
  const host = directTenantMutationEpoch(release);
  if (label === 'missing') return undefined;
  if (label === 'stale') return Math.max(0, host - 1);
  if (label === 'future') return host + 1;
  return host;
};
