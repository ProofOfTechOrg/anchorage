// SPDX-License-Identifier: Apache-2.0
// One resolution of the posture default, in a leaf that imports only types, so
// a module can read it without taking on the SDK barrel.
import type {
  ConnectorEgressPosture,
  PermissionManifest,
} from './contracts.js';

/** The omitted-field default: a manifest without the field declares only. */
export function resolveEgressPosture(
  manifest: PermissionManifest,
): ConnectorEgressPosture {
  return manifest.egressEnforcement ?? 'declaration-only';
}
