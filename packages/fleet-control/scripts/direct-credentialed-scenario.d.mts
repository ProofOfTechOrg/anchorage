// SPDX-License-Identifier: Apache-2.0

import type { FleetAuditResultRef } from '../src/fleet-audit-advance.js';
import type { CleanupTerminalReceipt } from '../src/types.js';
import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type {
  DirectInvocationAttempts,
  DirectInvocationClient,
} from './direct-credentialed-invocation.mjs';
import type {
  DirectSettlementEffect,
  DirectVerifiedExport,
  DirectWorkerVersionObservation,
} from './direct-credentialed-observations.mjs';
import type {
  DIRECT_SCENARIO_FAILURE_DETAILS,
  DIRECT_SCENARIO_FAILURES,
  DirectRunActionSummary,
  DirectRunJournal,
} from './direct-credentialed-run-state.mjs';
import type {
  DirectScenarioFootprint,
  DirectScenarioNormalRole,
  DirectScenarioOperationFacts,
  DirectScenarioPhase,
  DirectScenarioRecordFacts,
} from './direct-credentialed-scenario-checks.mjs';
import type { DirectFixtureRole } from './direct-credentialed-spec.js';

type Role = DirectFixtureRole;
type NormalRole = DirectScenarioNormalRole;

export type { DirectScenarioOperationSlot } from './direct-credentialed-scenario-checks.mjs';
export type { DirectScenarioPhase };
export type DirectScenarioFailure = (typeof DIRECT_SCENARIO_FAILURES)[number];
export type DirectScenarioFailureDetail =
  (typeof DIRECT_SCENARIO_FAILURE_DETAILS)[number];
interface ScenarioCall {
  readonly ordinal: number;
  readonly action: DirectRunActionSummary;
  readonly outcome:
    | 'prepared'
    | 'returned'
    | 'injected-response-loss'
    | 'reference-refused';
  readonly attempts: DirectInvocationAttempts | null;
  /** Settled force-terminal calls require a nullable before identity; prepared and other calls omit it. */
  readonly before?: Readonly<{ databaseId: string; scriptName: string }> | null;
  readonly migration: Readonly<{
    itemOrdinal: 0 | 1;
    cursor: number;
    step: string;
    itemsSha256: string;
  }> | null;
}
interface ScenarioProcess {
  readonly pid: number;
  readonly startTicks: string;
  readonly bootId: string;
}
interface ScenarioInventory {
  readonly operationId: string;
  readonly generation: number;
  readonly calls: number;
  readonly databaseIds: readonly string[];
  readonly namespaceIds: readonly string[];
  readonly scriptNames: readonly string[];
  readonly routeHostnames: readonly string[];
  readonly bucketNames: readonly string[];
  readonly findings: readonly Readonly<{
    kind: string;
    detailSha256: string;
  }>[];
}
type ScenarioAudit = FleetAuditResultRef &
  Readonly<{
    recordCount: 2;
    findings: readonly Readonly<{
      tenantTag: string;
      environment: string;
      kind: string;
      detailSha256: string;
    }>[];
  }>;
interface ScenarioFenceReading {
  readonly state: 'open' | 'draining' | 'migration-locked' | 'proof-only';
  readonly mutationEpoch: number;
  readonly requireMutationEpoch: boolean;
  readonly transitionRevision: number;
}
interface ScenarioFenceTransition {
  readonly before: ScenarioFenceReading;
  readonly after: ScenarioFenceReading | null;
  readonly ordinal: number | null;
}
interface ScenarioFenceSweep {
  readonly fence: ScenarioFenceReading;
  readonly categories: readonly Readonly<{
    category: string;
    class: 'work' | 'standing';
    empty: boolean;
  }>[];
  readonly observedAt: number;
  readonly ordinal: number;
}
export interface DirectScenarioFenceProofs {
  readonly drain: Readonly<Record<NormalRole, ScenarioFenceTransition | null>>;
  readonly sweeps: Readonly<
    Record<
      NormalRole,
      Readonly<{
        first: ScenarioFenceSweep;
        second: ScenarioFenceSweep | null;
        intervalMs: number | null;
      }> | null
    >
  >;
  readonly reopen: Readonly<Record<NormalRole, ScenarioFenceTransition | null>>;
  readonly probes: Readonly<
    Record<
      NormalRole,
      Readonly<{
        current: 'accepted';
        missing: 'missing';
        stale: 'stale';
        future: 'future';
        mutationEpoch: number;
        ordinal: number;
      }> | null
    >
  >;
}
export interface DirectScenarioProofs {
  readonly initial: Readonly<
    Record<Role, DirectWorkerVersionObservation | null>
  >;
  readonly candidate: Readonly<
    Record<NormalRole, DirectWorkerVersionObservation | null>
  >;
  readonly final: Readonly<
    Record<NormalRole, DirectWorkerVersionObservation | null>
  >;
  readonly objects: Readonly<
    Record<NormalRole, Readonly<{ size: number; sha256: string }> | null>
  >;
  readonly objectDeletions: Readonly<Record<NormalRole, number | null>>;
  readonly recoveryExportAbsent: Readonly<{
    beforeOrdinal: number | null;
    afterOrdinal: number | null;
  }>;
  readonly health: readonly Readonly<{
    role: Role;
    release: '1' | '2';
    marker: 'initial' | 'next';
    ordinal: number;
  }>[];
  readonly inventories: Readonly<
    Record<'before' | 'after', ScenarioInventory | null>
  >;
  readonly audits: Readonly<Record<'before' | 'after', ScenarioAudit | null>>;
  readonly fence: DirectScenarioFenceProofs;
  readonly restart: Readonly<{
    process: ScenarioProcess;
    resumedProcess: ScenarioProcess | null;
    lossOrdinal: number;
    operationId: string;
    witnessSha256: string;
    claimSha256: string;
    successorSha256: string;
    itemsSha256: string;
    replayOrdinal: number | null;
  }> | null;
  readonly steps: readonly Readonly<
    DirectInvocationAttempts & {
      ordinal: number;
      itemOrdinal: 0 | 1;
      step: string;
      beforeCursor: number;
      afterCursor: number;
    }
  >[];
  readonly effects: readonly DirectSettlementEffect[];
  readonly cleanup: CleanupTerminalReceipt | null;
  readonly exports: Readonly<Record<NormalRole, DirectVerifiedExport | null>>;
  readonly exportVerifications: readonly DirectVerifiedExport[];
  readonly decommission: Readonly<
    Record<
      NormalRole,
      Readonly<{
        operationId: string;
        databaseId: string;
        scriptName: string;
        phase: 'decommissioned';
      }> | null
    >
  >;
  readonly terminalForce: Readonly<{
    a: Readonly<{
      databaseId: string;
      scriptName: string;
      ordinal: number;
      attempts: DirectInvocationAttempts;
    }> | null;
  }>;
  readonly force: DirectScenarioFootprint | null;
  readonly residual: DirectScenarioFootprint | null;
}
export interface DirectScenarioState {
  readonly version: 1;
  readonly phase: DirectScenarioPhase;
  readonly startedOrdinal: number;
  readonly callCount: number;
  readonly phaseCalls: Readonly<Record<DirectScenarioPhase, number>>;
  readonly attempts: DirectInvocationAttempts;
  readonly sdkRequests: number;
  readonly inventoryCalls: Readonly<{ before: number; after: number }>;
  readonly lastCall: ScenarioCall | null;
  readonly mutation: ScenarioCall | null;
  readonly reconciledOrdinal: number;
  readonly operations: readonly DirectScenarioOperationFacts[];
  readonly records: readonly DirectScenarioRecordFacts[];
  readonly failure: Readonly<{
    code: DirectScenarioFailure;
    ordinal: number;
    detail?: DirectScenarioFailureDetail;
  }> | null;
  readonly proofs: DirectScenarioProofs;
}
export type DirectScenarioOutcome =
  | Readonly<{ status: 'restart-required' }>
  | Readonly<{
      status: 'complete';
      facts: DirectScenarioProofs;
      invocationCount: number;
      attempts: DirectInvocationAttempts;
      sdkRequests: number;
    }>
  | Readonly<{
      status: 'failed';
      reason: DirectScenarioFailure;
      detail?: DirectScenarioFailureDetail;
      phase: DirectScenarioPhase | null;
      invocationCount: number;
    }>;
export function runDirectCredentialedScenario(
  input: Readonly<{
    prepared: PreparedDirectConformance;
    journal: DirectRunJournal;
    invocation: DirectInvocationClient;
    apiToken: string;
    fetch?: typeof fetch;
  }>,
): Promise<DirectScenarioOutcome>;
