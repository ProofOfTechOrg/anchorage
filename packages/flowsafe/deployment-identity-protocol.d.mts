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

/** The table the single deployment execution fence row lives in. */
export const EXECUTION_FENCE_TABLE: 'flowsafe_execution_fence';
/** The fence row's fixed primary key — one deployment, one database, one row. */
export const EXECUTION_FENCE_ROW_ID: 'deployment';
/** Every fence state, ordered from most to least permissive. */
export const EXECUTION_FENCE_STATES: readonly [
  'open',
  'draining',
  'migration-locked',
  'proof-only',
];
/** The states a deployment may be BORN in. */
export const INITIAL_EXECUTION_FENCE_STATES: readonly [
  'open',
  'migration-locked',
];
/**
 * The fence table's schema. `do-runner/execution-fence.ts` issues this exact
 * string, so the store and the provisioning protocol cannot create differently
 * shaped tables.
 */
export const EXECUTION_FENCE_DDL: string;

/** The fence state a deployment is provisioned into. Required; no default. */
export type InitialExecutionFenceState =
  (typeof INITIAL_EXECUTION_FENCE_STATES)[number];

export class DeploymentIdentityError extends Error {}

/**
 * Validate the fence state a deployment is to be born in, throwing
 * DeploymentIdentityError on anything else.
 */
export function assertInitialExecutionFenceState(
  state: unknown,
  caller: string,
): InitialExecutionFenceState;

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
  options: {
    caller?: string;
    provisionedAt?: string;
    /** Injectable clock (epoch milliseconds) for the seeded rows. */
    now?: () => number;
    /**
     * The fence state the deployment is born in. REQUIRED and without a
     * default: a migration host that forgot to ask for 'migration-locked'
     * would otherwise silently get an executing deployment.
     */
    initialExecutionFenceState: InitialExecutionFenceState;
  },
): Promise<void>;
