// SPDX-License-Identifier: Apache-2.0

// Usage:
//   node scripts/record-audit-baseline.mjs --check
//   node scripts/record-audit-baseline.mjs --write

import { runBaselineRecorder } from './baseline-recorder.mjs';

await runBaselineRecorder({
  scriptUrl: import.meta.url,
  noun: 'audit',
  worldModule: 'packages/fleet-control/test/fixtures/fleet-audit-world.ts',
  baselineFile: 'packages/fleet-control/test/fixtures/fleet-audit-baseline.ts',
  run: (world) => world.runFleetAuditBaseline(),
  imports: `import type { DriftFinding } from '../../src/fleet.js';
import type { AuditOpLogEntry } from './fleet-audit-world.js';`,
  exports: [
    {
      name: 'AUDIT_BASELINE_FINDINGS',
      key: 'findings',
      satisfies: 'readonly DriftFinding[]',
    },
    {
      name: 'AUDIT_BASELINE_OPS',
      key: 'ops',
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
