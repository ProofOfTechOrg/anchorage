// SPDX-License-Identifier: Apache-2.0

import constants from './contract.json';

/**
 * The one source of truth for the values shared between this starter's
 * conformance artifacts and the operator configuration handed to
 * `pnpm fleet-control:credentialed`. `scripts/emit-conformance-config.mjs`
 * renders that JSON from the same file, so an artifact and its configuration
 * cannot disagree about a binding name, a path, or a Durable Object class.
 *
 * The contract itself is defined by `docs/fleet-control.md` under "Implement
 * the artifact contract" and enforced by
 * `packages/fleet-control/scripts/credentialed-conformance.mjs`. This file only
 * records the choices this starter makes inside it.
 */

export interface ConformanceBinding {
  readonly name: string;
  readonly className: string;
}

export interface ConformanceContract {
  readonly contractVersion: number;
  readonly httpPath: string;
  readonly webSocketPath: string;
  readonly candidateMainModule: string;
  readonly stateMainModule: string;
  readonly compatibilityDate: string;
  readonly compatibilityFlags: readonly string[];
  readonly cpuLimitMs: number;
  readonly subrequestLimit: number;
  readonly schemaVersion: number;
  readonly applicationVariableName: string;
  readonly applicationVariableValue: string;
  readonly applicationSecretBinding: string;
  readonly applicationR2Binding: string;
  readonly auditProxyClassName: string;
  /**
   * Fleet control gives this ONE array to both roles: the candidate receives
   * each entry as a remote binding into the state script, and the state script
   * receives the same names as its own local namespaces
   * (`cloudflare-client.ts`, `uploadDispatchWorker` and
   * `uploadNamespacedStateWorker`). So `MAINTENANCE` and `RUNNER` are here
   * because trusted state needs them, and the candidate holds bindings it never
   * calls. That is the platform's contract, not a choice this file can narrow;
   * the write boundary is the approval grant, not the binding set.
   */
  readonly durableObjectBindings: readonly ConformanceBinding[];
  readonly newDurableObjectBinding: ConformanceBinding;
  readonly stateMigrationTags: Readonly<{ v1: string; v2: string }>;
  /** Fixed Durable Object instance names both roles address. */
  readonly stateInstanceName: string;
  readonly v2InstanceName: string;
}

// An annotation, never `as`: an assertion would let a rename in the JSON
// compile to `undefined` at every reader, while this reports the missing field
// at build time. The runtime guard below covers only the three names `Env`
// declares.
export const CONFORMANCE_CONTRACT: ConformanceContract = constants;

/**
 * Derived, never a second literal: the artifacts answer with this and
 * `scripts/emit-conformance-config.mjs` writes the JSON value into the operator
 * configuration, so a bump has to move both together or neither.
 */
export const CONFORMANCE_CONTRACT_VERSION =
  CONFORMANCE_CONTRACT.contractVersion;

/**
 * `Env` declares these three as named fields rather than reaching through an
 * index signature, so a rename in the JSON must fail here instead of silently
 * probing `undefined` and reporting it to the gate as a contract violation.
 * Same fail-fast intent as `assertWorkflowsRegistered` in `src/workflows.ts`.
 */
if (
  CONFORMANCE_CONTRACT.applicationVariableName !== 'APPLICATION_MODE' ||
  CONFORMANCE_CONTRACT.applicationR2Binding !== 'APPLICATION_FILES' ||
  CONFORMANCE_CONTRACT.applicationSecretBinding !==
    'APPLICATION_CONFORMANCE_SECRET'
) {
  throw new Error(
    'conformance contract renamed an application binding that Env declares by name',
  );
}

/** The approval-gated workflow the FlowSafe actions drive. */
export const CONFORMANCE_WORKFLOW_ID = 'conformance-approval';

/**
 * Bookkeeping identity for both halves of the trusted state script — the
 * composed Worker's maintenance attribution and the approval bridge's record
 * creator. One constant because they write to the same D1 approval store, and
 * two literals would split one deployment's audit trail across two names.
 */
export const CONFORMANCE_SYSTEM_PRINCIPAL_ID = 'anchorage-conformance';

/**
 * Shared by both roles and the WebSocket surface: a missing or empty field is a
 * caller error, and the message names the field so a failed probe is legible in
 * the gate's output.
 */
export function requiredText(value: unknown, field: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${field} is required`);
  }
  return value;
}
