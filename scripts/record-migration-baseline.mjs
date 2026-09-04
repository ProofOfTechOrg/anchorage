// SPDX-License-Identifier: Apache-2.0

// Records the golden baselines of `migrateFleet()` (src/fleet.ts, before its
// R4-C.2 decomposition into a bounded frozen-plan executor), for the two
// hand-authored worlds in
// packages/fleet-control/test/fixtures/fleet-migration-worlds.ts:
//   - the SUCCESS world: the exact records `migrateFleet()` returns AND the
//     exact sequence of calls it makes onto its `store`, `backendFor`,
//     `specFor`, `secretsFor`, and `settlementFor` collaborators (the "op log");
//   - the STOP world: the exact refusal it rejects with, beside the op log that
//     proves first-error stop parity — the first record's whole body, the
//     refused record's two-token preamble prefix, and nothing at all for the
//     record the drain never reaches.
//
// The baselines must be recorded from PRE-REWRITE code, so this script writes
// exactly one file — the generated literals — and never touches the worlds it
// drives. `--check` re-derives all four values from the unchanged worlds and
// compares them STRUCTURALLY against the committed module's exports, so the
// compatibility gate never depends on formatter behavior; it writes nothing.
//
// This script is a re-recording aid, not a CI gate: the in-suite equivalence
// titles in packages/fleet-control/test/fleet-migration-golden.test.ts are the
// automatic behavioral gate.
//
// Usage:
//   node scripts/record-migration-baseline.mjs            # write the baseline
//   node scripts/record-migration-baseline.mjs --check    # verify, exit 1 on drift

import { runBaselineRecorder } from './baseline-recorder.mjs';

await runBaselineRecorder({
  scriptUrl: import.meta.url,
  noun: 'migration',
  worldModule: 'packages/fleet-control/test/fixtures/fleet-migration-worlds.ts',
  baselineFile:
    'packages/fleet-control/test/fixtures/fleet-migration-baseline.ts',
  run: async (world) => {
    const success = await world.runFleetMigrationSuccessBaseline();
    const stop = await world.runFleetMigrationStopBaseline();
    return {
      successResult: success.result,
      successOps: success.ops,
      stopError: stop.error,
      stopOps: stop.ops,
    };
  },
  header: `/**
 * GENERATED FILE — DO NOT EDIT BY HAND.
 *
 * Written by \`scripts/record-migration-baseline.mjs\` from the hand-authored
 * worlds in \`fleet-migration-worlds.ts\`. It freezes the observable behavior of
 * \`migrateFleet()\` (src/fleet.ts) before it is decomposed into a bounded
 * frozen-plan executor, so the decomposition can be proven byte-equivalent.
 * Verify with \`node scripts/record-migration-baseline.mjs --check\`; any
 * required change to these literals is a compatibility break, not a fixture
 * update.
 */`,
  imports: `import type { FleetRecord } from '../../src/types.js';
import type { MigrationOpLogEntry } from './fleet-migration-worlds.js';`,
  exports: [
    {
      name: 'MIGRATION_SUCCESS_BASELINE_RESULT',
      key: 'successResult',
      jsDoc: `/** Every record \`migrateFleet()\` returned for the success world, in order. */`,
      satisfies: 'readonly FleetRecord[]',
    },
    {
      name: 'MIGRATION_SUCCESS_BASELINE_OPS',
      key: 'successOps',
      jsDoc: `/**
 * Every collaborator call \`migrateFleet()\` made for the success world, in
 * order: the store's \`withDeploymentLease\`/\`get\`/\`put:<phase-or-subphase>\`
 * and \`lease.assertOwned()\`, every \`resolver:<kind>:<key>\` invocation, every
 * backend call, and every settlement. The finalized-state provider's six
 * tokens and the state reconcile's \`put:upload-authorized\`/\`put:uploaded\` are
 * in \`MigrationOpLogEntry\`'s vocabulary but never appear here: no world holds
 * a finalized-ordinary-plane record.
 */`,
      satisfies: 'readonly MigrationOpLogEntry[]',
    },
    {
      name: 'MIGRATION_STOP_BASELINE_ERROR',
      key: 'stopError',
      jsDoc: `/** The exact refusal \`migrateFleet()\` rejected the stop world with. */`,
      satisfies: 'string',
    },
    {
      name: 'MIGRATION_STOP_BASELINE_OPS',
      key: 'stopOps',
      jsDoc: `/**
 * Every collaborator call \`migrateFleet()\` made for the stop world before it
 * rejected, in order. First-error stop parity is the SHAPE of this log: the
 * first record's whole body, then the refused record's \`withDeploymentLease\`
 * and \`get\` — the two calls its refusal fires after — and then nothing, because
 * the drain never reaches the third record.
 */`,
      satisfies: 'readonly MigrationOpLogEntry[]',
    },
  ],
  summary: (baseline) =>
    `${baseline.successResult.length} migrated records, ` +
    `${baseline.successOps.length} success ops, ` +
    `${baseline.stopOps.length} stop ops`,
});
