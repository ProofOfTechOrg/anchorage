// SPDX-License-Identifier: Apache-2.0
// The root Cloudflare test toolchain uses Workers types v5. This erased
// assertion keeps FlowSafe's structural R2 seam compatible with that host.
import type { R2Bucket } from '@cloudflare/workers-types';

import type { ArtifactBucket } from '../packages/flowsafe/src/artifacts/index.js';

type AssertTrue<T extends true> = T;
type _R2V5SatisfiesArtifactBucket = AssertTrue<
  R2Bucket extends ArtifactBucket ? true : false
>;
