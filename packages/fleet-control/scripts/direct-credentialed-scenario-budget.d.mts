// SPDX-License-Identifier: Apache-2.0

export const DIRECT_SCENARIO_PHASES: readonly [
  'provision-a',
  'provision-b',
  'inventory-before',
  'audit-before',
  'migration-start',
  'migration-interrupt',
  'migration-restart',
  'migration',
  'post-migration',
  'inventory-after',
  'audit-after',
  'failed-recovery',
  'cleanup-recovery',
  'provision-recovery',
  'delete-objects',
  'decommission-a',
  'decommission-b',
  'force-recovery',
  'force-observe',
  'recover-force-residual',
  'complete',
];

export interface DirectScenarioPhaseBudget {
  readonly measured: number;
  readonly ceiling: number;
  readonly reserve: number;
}

export const DIRECT_SCENARIO_INVOCATION_BUDGET: Readonly<
  Record<(typeof DIRECT_SCENARIO_PHASES)[number], DirectScenarioPhaseBudget>
>;

export const DIRECT_SCENARIO_MIN_INVOCATIONS: number;
