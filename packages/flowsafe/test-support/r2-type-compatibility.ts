// SPDX-License-Identifier: Apache-2.0
// FlowSafe retains Workers types v4 while @mastra/cloudflare-d1 peers on v4.
// This erased assertion proves its public structural seam accepts that R2.
import type { R2Bucket } from '@cloudflare/workers-types';

import type { ArtifactBucket } from '../src/artifacts/index.js';

type AssertTrue<T extends true> = T;
type _R2V4SatisfiesArtifactBucket = AssertTrue<
  R2Bucket extends ArtifactBucket ? true : false
>;
