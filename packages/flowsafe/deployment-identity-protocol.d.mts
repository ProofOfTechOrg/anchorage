// SPDX-License-Identifier: Apache-2.0

export interface DeploymentIdentityProtocolStatement {
  readonly mode: 'read' | 'write';
  readonly sql: string;
  readonly bindings: readonly unknown[];
}

export type DeploymentIdentityProtocolRow = Readonly<Record<string, unknown>>;

export type DeploymentIdentityProtocolExecutor = (
  statement: DeploymentIdentityProtocolStatement,
) => Promise<readonly DeploymentIdentityProtocolRow[]>;

export const DEPLOYMENT_TAG_PATTERN: RegExp;
export const DEPLOYMENT_SENTINEL_TABLE: 'flowsafe_deployment';
export const DEPLOYMENT_SENTINEL_DDL: string;
export const DEPLOYMENT_SENTINEL_COLUMNS: readonly Readonly<{
  name: string;
  type: string;
  notnull: number;
  pk: number;
}>[];

export class DeploymentIdentityError extends Error {}

export function assertValidDeploymentTag(
  tag: unknown,
  caller: string,
): asserts tag is string;

export function normalizeDeploymentSentinelSql(sql: string): string;

export function deploymentIdentityApplicationTables(
  rows: readonly DeploymentIdentityProtocolRow[],
): string[];

export function readDeploymentIdentityProtocol(
  execute: DeploymentIdentityProtocolExecutor,
): Promise<string | undefined>;

export function provisionDeploymentIdentityProtocol(
  execute: DeploymentIdentityProtocolExecutor,
  tag: string,
  options?: {
    caller?: string;
    provisionedAt?: string;
  },
): Promise<void>;
