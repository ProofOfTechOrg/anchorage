// SPDX-License-Identifier: Apache-2.0
// The manifests and cases the `src` conformance suites drive. Keep vitest
// out of it: it sits under `src`, and the build program excludes it by name
// (`tsconfig.json`'s `exclude`) rather than by the `.test.ts` suffix. That
// exclusion, and its own program and vitest project, are why the workers suite
// under `worker-tests/` carries copies of these declarations instead of
// importing them.
//
// The imports name the `connector-sdk` leaves, not the barrel, which imports
// this directory's modules back.
import type { ConnectorConfig, PermissionManifest } from './contracts.js';
import type { ConnectorConformanceCase } from './egress-conformance.js';

export type Execute = ConnectorConfig<unknown, unknown>['execute'];

export const manifest: PermissionManifest = {
  sideEffect: 'read',
  egress: ['api.vendor.example'],
  egressEnforcement: 'enforced',
};
export const noEgress: PermissionManifest = {
  sideEffect: 'read',
  egressEnforcement: 'enforced',
};
export const requestCase: ConnectorConformanceCase = {
  name: 'request',
  input: {},
  expect: { outcome: 'guarded-request', hosts: ['api.vendor.example'] },
};
export const quietCase: ConnectorConformanceCase = {
  name: 'quiet',
  input: {},
  expect: { outcome: 'no-network' },
};
