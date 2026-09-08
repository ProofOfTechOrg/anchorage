// SPDX-License-Identifier: Apache-2.0

// Usage:
//   node scripts/record-drain-baseline.mjs --check
//   node scripts/record-drain-baseline.mjs --write

import { runBaselineRecorder } from './baseline-recorder.mjs';

await runBaselineRecorder({
  scriptUrl: import.meta.url,
  noun: 'drain',
  worldModule:
    'packages/fleet-control/test/fixtures/fleet-inventory-drain-world.ts',
  baselineFile:
    'packages/fleet-control/test/fixtures/fleet-inventory-drain-baseline.ts',
  run: (world) => world.runFleetInventoryDrain(),
  imports: `import type { FleetResourceInventory } from '../../src/types.js';
import type { DrainRequestRecord } from './fleet-inventory-drain-world.js';`,
  exports: [
    {
      name: 'DRAIN_BASELINE_REQUESTS',
      key: 'requests',
      satisfies: 'readonly DrainRequestRecord[]',
    },
    {
      name: 'DRAIN_BASELINE_INVENTORY',
      key: 'inventory',
      satisfies: 'FleetResourceInventory',
    },
  ],
  summary: (drain) =>
    `${drain.requests.length} requests, ` +
    `${drain.inventory.findings.length} findings, ` +
    `${drain.inventory.deployments.length} deployments, ` +
    `${drain.inventory.routes.length} routes`,
});
