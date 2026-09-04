// SPDX-License-Identifier: Apache-2.0

// Records the golden baseline of
// CloudflareProvisioningClient.collectFleetInventory(): the full provider
// request sequence and the exact FleetResourceInventory it returns for the
// hand-authored world in
// packages/fleet-control/test/fixtures/fleet-inventory-drain-world.ts.
//
// The baseline must be recorded from PRE-REWRITE code, so this script writes
// exactly one file — the generated literals — and never touches the world it
// drives. `--check` re-derives both values from the unchanged world and
// compares them STRUCTURALLY against the committed module's exports, so the
// compatibility gate never depends on formatter behavior; it writes nothing.
//
// This script is a re-recording aid, not a CI gate: the in-suite equivalence
// title in packages/fleet-control/test/cloudflare-client.test.ts is the
// automatic behavioral gate.
//
// Usage:
//   node scripts/record-drain-baseline.mjs            # write the baseline
//   node scripts/record-drain-baseline.mjs --check    # verify, exit 1 on drift

import { runBaselineRecorder } from './baseline-recorder.mjs';

await runBaselineRecorder({
  scriptUrl: import.meta.url,
  noun: 'drain',
  worldModule:
    'packages/fleet-control/test/fixtures/fleet-inventory-drain-world.ts',
  baselineFile:
    'packages/fleet-control/test/fixtures/fleet-inventory-drain-baseline.ts',
  run: (world) => world.runFleetInventoryDrain(),
  header: `/**
 * GENERATED FILE — DO NOT EDIT BY HAND.
 *
 * Written by \`scripts/record-drain-baseline.mjs\` from the hand-authored world
 * in \`fleet-inventory-drain-world.ts\`. It freezes the observable behavior of
 * \`CloudflareProvisioningClient.collectFleetInventory()\` before its internals
 * are rewritten, so the rewrite can be proven byte-equivalent. Verify with
 * \`node scripts/record-drain-baseline.mjs --check\`; any required change to
 * these literals is a compatibility break, not a fixture update.
 */`,
  imports: `import type { FleetResourceInventory } from '../../src/types.js';
import type { DrainRequestRecord } from './fleet-inventory-drain-world.js';`,
  exports: [
    {
      name: 'DRAIN_BASELINE_REQUESTS',
      key: 'requests',
      jsDoc: '/** Every provider request the drain issued, in order. */',
      satisfies: 'readonly DrainRequestRecord[]',
    },
    {
      name: 'DRAIN_BASELINE_INVENTORY',
      key: 'inventory',
      jsDoc: '/** The exact inventory the drain returned. */',
      satisfies: 'FleetResourceInventory',
    },
  ],
  summary: (drain) =>
    `${drain.requests.length} requests, ` +
    `${drain.inventory.findings.length} findings, ` +
    `${drain.inventory.deployments.length} deployments, ` +
    `${drain.inventory.routes.length} routes`,
});
