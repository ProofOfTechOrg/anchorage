// SPDX-License-Identifier: Apache-2.0

export const DIRECT_TENANT_OBJECT_KEY = 'direct-conformance-fixture';
export const DIRECT_TENANT_OBJECT_BODY = 'direct-conformance-fixture-data';

// The caller epoch an artifact of this release carries. Release 2 is the
// post-cutover artifact; release 1 predates the activation and is therefore
// stale once the control plane advances the fence. One definition, because §4 C2
// is precisely a comparison between the tenant's configured value and the
// fixture's re-derivation of it, and two copies can drift while both lanes pass.
export const directTenantMutationEpoch = (release) => (release === '2' ? 1 : 0);
