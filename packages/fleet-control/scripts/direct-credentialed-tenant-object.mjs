// SPDX-License-Identifier: Apache-2.0

export const DIRECT_TENANT_OBJECT_KEY = 'direct-conformance-fixture';
export const DIRECT_TENANT_OBJECT_BODY = 'direct-conformance-fixture-data';
export const DIRECT_CONTINUATION_WORKFLOW = 'direct-continuation-proof';
export const DIRECT_CONTINUATION_STEP = 'hold';

// The `/__direct/*` routes the tenant Worker answers. The router and the
// reference clients import these values, so a rename is one edit for them.
export const DIRECT_TENANT_ROUTES = Object.freeze({
  health: '/__direct/health',
  object: '/__direct/object',
  fenceMutate: '/__direct/fence-mutate',
  fenceProbe: '/__direct/fence-probe',
});

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
