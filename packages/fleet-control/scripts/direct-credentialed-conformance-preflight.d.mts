// SPDX-License-Identifier: Apache-2.0

import type {
  DirectConformanceConfig,
  DirectConformanceNames,
  DirectRuntimeIntent,
} from './direct-credentialed-conformance-config.mjs';

export const DIRECT_MANIFEST_MODULE: 'direct-run-manifest.js';
export const DIRECT_MAX_UPLOAD_BYTES: number;

export interface DirectModuleSnapshot {
  readonly name: string;
  readonly source: string;
  readonly contentType: 'application/javascript+module';
  readonly byteLength: number;
  readonly sha256: string;
}

export interface DirectWasmSnapshot {
  readonly name: string;
  readonly base64: string;
  readonly contentType: 'application/wasm';
  readonly byteLength: number;
  readonly sha256: string;
}

export interface DirectRunManifest {
  readonly contractVersion: 1;
  readonly configSha256: string;
  readonly resourcePrefix: string;
  readonly environment: string;
  readonly names: DirectConformanceNames;
  readonly referenceRuntime: Omit<
    DirectConformanceConfig['referenceWorker'],
    'artifact'
  >;
  readonly deploymentRuntime: Omit<DirectRuntimeIntent, 'artifact'>;
  readonly tenantModule: DirectModuleSnapshot;
  readonly tenantWasm: readonly DirectWasmSnapshot[];
  readonly fixtureVersion: 1;
  readonly interruption: 'after-migration-admission';
}

export interface PreparedDirectConformance {
  readonly config: DirectConformanceConfig;
  readonly names: DirectConformanceNames;
  readonly configSha256: string;
  readonly manifest: DirectRunManifest;
  readonly referenceModules: readonly [
    DirectModuleSnapshot,
    DirectModuleSnapshot,
    ...DirectWasmSnapshot[],
  ];
  readonly referenceUploadBytes: number;
  readonly referenceModuleSetSha256: string;
}

export function readDirectConformanceConfig(
  input: Readonly<{
    configPath: string;
    now?: number;
  }>,
): Promise<
  Readonly<{
    configPath: string;
    configBytes: Uint8Array;
    config: DirectConformanceConfig;
  }>
>;

export function preflightDirectConformance(
  input: Readonly<{
    configPath: string;
    now?: number;
  }>,
): Promise<PreparedDirectConformance>;
