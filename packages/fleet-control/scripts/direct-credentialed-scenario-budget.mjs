// SPDX-License-Identifier: Apache-2.0

// `measured` is what a phase spends in one uninterrupted end-to-end run, recorded
// as data. `ceiling` is 2x measured rounded up to a multiple of 8, minimum 16: the
// cumulative cap on one phase across resumes, wide enough for re-entries that each
// repeat the entry `sync()` and its observation. Ceilings bound a runaway phase and
// may sum past the configured budget, because they are a cap and not a reservation.
// `reserve` is 1.25x measured rounded up to a multiple of 4, minimum 4.
// DIRECT_SCENARIO_MIN_INVOCATIONS adds the bootstrap control read and resume
// headroom to the sum of every `reserve`: it is the floor
// `referenceWorker.maxInvocations` clears.
const MEASURED = Object.freeze({
  'provision-a': 10,
  'provision-b': 9,
  'inventory-before': 22,
  'audit-before': 57,
  'fence-drain': 9,
  'migration-start': 3,
  'migration-interrupt': 5,
  'migration-restart': 7,
  migration: 105,
  'post-migration': 5,
  'fence-reopen': 9,
  'fence-proofs': 9,
  'inventory-after': 22,
  'audit-after': 57,
  'failed-recovery': 4,
  'cleanup-recovery': 12,
  'provision-recovery': 7,
  'delete-objects': 7,
  'decommission-a': 70,
  'decommission-b': 70,
  'force-terminal-a': 4,
  'force-recovery': 6,
  'force-observe': 3,
  'recover-force-residual': 3,
  complete: 1,
});

const roundUp = (value, step) => Math.ceil(value / step) * step;

export const DIRECT_SCENARIO_INVOCATION_BUDGET = Object.freeze(
  Object.fromEntries(
    Object.entries(MEASURED).map(([phase, measured]) => [
      phase,
      Object.freeze({
        measured,
        ceiling: Math.max(16, roundUp(2 * measured, 8)),
        reserve: Math.max(4, roundUp(1.25 * measured, 4)),
      }),
    ]),
  ),
);

export const DIRECT_SCENARIO_PHASES = Object.freeze(
  Object.keys(DIRECT_SCENARIO_INVOCATION_BUDGET),
);

export const DIRECT_SCENARIO_MIN_INVOCATIONS =
  Object.values(DIRECT_SCENARIO_INVOCATION_BUDGET).reduce(
    (total, entry) => total + entry.reserve,
    0,
  ) +
  1 +
  8;
