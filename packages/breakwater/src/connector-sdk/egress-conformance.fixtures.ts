// SPDX-License-Identifier: Apache-2.0
// The manifests and cases both conformance suites drive. Keep vitest out of
// it: it sits under `src`, and the build program excludes it by name rather
// than by the `.test.ts` suffix.
import type {
  ConnectorConfig,
  ConnectorConformanceCase,
  PermissionManifest,
} from './index.js';

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
