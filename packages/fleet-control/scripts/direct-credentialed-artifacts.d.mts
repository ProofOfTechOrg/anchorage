// SPDX-License-Identifier: Apache-2.0

import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';

export interface DirectArtifactBuildMeasurement {
  readonly metafilePath: string;
  readonly rawBytes: number;
  readonly gzipBytes: number;
  readonly sha256: string;
}

export interface BuiltDirectConformanceArtifacts {
  readonly configPath: string;
  readonly prepared: PreparedDirectConformance;
  readonly builds: Readonly<{
    reference: DirectArtifactBuildMeasurement;
    tenant: DirectArtifactBuildMeasurement;
  }>;
}

export function buildDirectConformanceArtifacts(
  input: Readonly<{
    configPath: string;
    outputDirectory: string;
    now?: number;
  }>,
): Promise<BuiltDirectConformanceArtifacts>;
