// SPDX-License-Identifier: Apache-2.0

export const DIRECT_CONFORMANCE_CONTRACT_VERSION: 1;

export interface DirectArtifactIntent {
  readonly bundle: string;
  readonly mainModule: string;
  readonly sha256: string;
}

export interface DirectRuntimeIntent {
  readonly artifact: DirectArtifactIntent;
  readonly compatibilityDate: string;
  readonly compatibilityFlags: readonly [] | readonly ['nodejs_compat'];
  readonly cpuLimitMs: number;
  readonly subrequestLimit: number;
}

export interface DirectConformanceConfig {
  readonly contractVersion: 1;
  readonly disposableAccount: boolean;
  readonly resourcePrefix: string;
  readonly environment: string;
  readonly ownedHostname: string;
  readonly referenceWorker: DirectRuntimeIntent &
    Readonly<{
      requestTimeoutMs: number;
      invocationTimeoutMs: number;
      maxProviderRequests: number;
      maxInvocations: number;
    }>;
  readonly deployment: DirectRuntimeIntent &
    Readonly<{
      spec: Readonly<{ fixtureVersion: 1 }>;
    }>;
  readonly interruption: 'after-migration-admission';
}

export interface DirectDeploymentNames {
  readonly tenantTag: string;
  readonly scriptName: string;
  readonly databaseName: string;
  readonly routeHostname: string;
}

export interface DirectConformanceNames {
  readonly referenceWorker: string;
  readonly fleetDatabase: string;
  readonly quotaDatabase: string;
  readonly exportBucket: string;
  readonly referenceHostname: string;
  readonly roles: Readonly<
    Record<'a' | 'b' | 'recovery', DirectDeploymentNames>
  >;
}

export function validateDirectConformanceConfig(
  value: unknown,
  options?: Readonly<{ now?: number }>,
): DirectConformanceConfig;

export function deriveDirectConformanceNames(
  config: Pick<DirectConformanceConfig, 'resourcePrefix' | 'ownedHostname'>,
): DirectConformanceNames;
