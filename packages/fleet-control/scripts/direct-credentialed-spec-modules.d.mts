// SPDX-License-Identifier: Apache-2.0

import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import type {
  DirectFixtureRelease,
  DirectFixtureRole,
} from './direct-credentialed-spec.js';

export function directDeploymentModules(
  manifest: DirectRunManifest,
  role: DirectFixtureRole,
  release: DirectFixtureRelease,
): readonly Readonly<{
  name: string;
  content: string | Uint8Array;
  contentType: string;
}>[];
