// SPDX-License-Identifier: Apache-2.0

export interface DeploymentIdentityProtocolStatement {
  readonly mode: 'read' | 'write';
  readonly sql: string;
  readonly bindings: readonly string[];
}

export type DeploymentIdentityProtocolRow = Readonly<Record<string, unknown>>;

export type DeploymentIdentityProtocolExecutor = (
  statement: DeploymentIdentityProtocolStatement,
) => Promise<readonly DeploymentIdentityProtocolRow[]>;

export const DEPLOYMENT_TAG_PATTERN: RegExp;
export const DEPLOYMENT_ENVIRONMENT_PATTERN: RegExp;
export const DEPLOYMENT_SENTINEL_TABLE: 'flowsafe_deployment';
/**
 * Internal Worker-to-Durable-Object credential header. Topology helpers always
 * overwrite it, and public request resolvers reject it. The value is a
 * deployment secret rather than the public deployment tag.
 */
export const DEPLOYMENT_IDENTITY_HEADER: 'x-flowsafe-deployment-identity';
export const DEPLOYMENT_SENTINEL_DDL: string;
export const DEPLOYMENT_SENTINEL_COLUMNS: readonly Readonly<{
  name: string;
  type: string;
  notnull: number;
  pk: number;
}>[];

export class DeploymentIdentityError extends Error {}

export function assertDeploymentIdentitySecret(
  secret: unknown,
  caller?: string,
): asserts secret is string;

/** Stamp the internal credential onto an ordinary topology request. */
export function deploymentIdentityHeaders(
  secret: string,
  initial?: HeadersInit,
): Record<string, string>;

export function isDeploymentEnvironment(value: unknown): value is string;

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
