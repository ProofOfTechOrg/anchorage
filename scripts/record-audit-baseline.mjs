// SPDX-License-Identifier: Apache-2.0

// Records the golden baseline of `auditFleetDrift()` (src/fleet.ts, before its
// R4-B.2 decomposition into bounded stages): the exact findings array it
// returns AND the exact sequence of calls it makes onto its `store`,
// `backendFor`, `specFor`, and `maintenanceSecretFor` collaborators (the "op
// log"), for the hand-authored world in
// packages/fleet-control/test/fixtures/fleet-audit-world.ts.
//
// The baseline must be recorded from PRE-REWRITE code, so this script writes
// exactly one file — the generated literals — and never touches the world it
// drives. `--check` re-derives both values from the unchanged world and
// compares them STRUCTURALLY against the committed module's exports, so the
// compatibility gate never depends on formatter behavior; it writes nothing.
//
// This script is a re-recording aid, not a CI gate: the in-suite equivalence
// title in packages/fleet-control/test/fleet-audit-golden.test.ts is the
// automatic behavioral gate.
//
// Usage:
//   node scripts/record-audit-baseline.mjs            # write the baseline
//   node scripts/record-audit-baseline.mjs --check    # verify, exit 1 on drift

import { runBaselineRecorder } from './baseline-recorder.mjs';

await runBaselineRecorder({
  scriptUrl: import.meta.url,
  noun: 'audit',
  worldModule: 'packages/fleet-control/test/fixtures/fleet-audit-world.ts',
  baselineFile: 'packages/fleet-control/test/fixtures/fleet-audit-baseline.ts',
  run: (world) => world.runFleetAuditBaseline(),
  header: `/**
 * GENERATED FILE — DO NOT EDIT BY HAND.
 *
 * Written by \`scripts/record-audit-baseline.mjs\` from the hand-authored world
 * in \`fleet-audit-world.ts\`. It freezes the observable behavior of
 * \`auditFleetDrift()\` (src/fleet.ts) before it is decomposed into bounded
 * stages, so the decomposition can be proven byte-equivalent. Verify with
 * \`node scripts/record-audit-baseline.mjs --check\`; any required change to
 * these literals is a compatibility break, not a fixture update.
 */`,
  imports: `import type { DriftFinding } from '../../src/fleet.js';
import type { AuditOpLogEntry } from './fleet-audit-world.js';`,
  exports: [
    {
      name: 'AUDIT_BASELINE_FINDINGS',
      key: 'findings',
      jsDoc: '/** Every finding `auditFleetDrift()` returned, in order. */',
      satisfies: 'readonly DriftFinding[]',
    },
    {
      name: 'AUDIT_BASELINE_OPS',
      key: 'ops',
      jsDoc: `/**
 * Every \`withDeploymentLease\`/\`get\`/\`put\`/\`inspect\`/\`ensureMaintenance\`
 * call, every \`resolver:<kind>\` invocation, and every \`lease.assertOwned()\`
 * call \`auditFleetDrift()\` made, in order. \`list\`/\`renew\`/\`delete\` are in
 * \`AuditOpLogEntry\`'s vocabulary but never appear here (defensive, unused by
 * this pre-decomposition world).
 */`,
      satisfies: 'readonly AuditOpLogEntry[]',
    },
  ],
  summary: (baseline) => {
    const distinctKinds = new Set(
      baseline.findings.map((finding) => finding.kind),
    ).size;
    return (
      `${baseline.findings.length} findings (${distinctKinds} distinct kinds), ` +
      `${baseline.ops.length} ops`
    );
  },
});
