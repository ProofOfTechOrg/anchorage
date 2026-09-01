// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  AUDIT_BASELINE_FINDINGS,
  AUDIT_BASELINE_OPS,
} from './fixtures/fleet-audit-baseline.js';
import { runFleetAuditBaseline } from './fixtures/fleet-audit-world.js';

describe('fleet audit golden baseline', () => {
  it('audits the recorded world into the frozen golden findings and op log, assertOwned included', async () => {
    const { findings, ops } = await runFleetAuditBaseline();

    expect(findings).toStrictEqual(AUDIT_BASELINE_FINDINGS);
    expect(ops).toStrictEqual(AUDIT_BASELINE_OPS);
  });
});
