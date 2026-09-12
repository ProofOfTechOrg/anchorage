// SPDX-License-Identifier: Apache-2.0

export interface DirectScenarioPhaseBudget {
  readonly measured: number;
  readonly ceiling: number;
  readonly reserve: number;
}

export const DIRECT_SCENARIO_INVOCATION_BUDGET: Readonly<{
  'provision-a': DirectScenarioPhaseBudget;
  'provision-b': DirectScenarioPhaseBudget;
  'inventory-before': DirectScenarioPhaseBudget;
  'audit-before': DirectScenarioPhaseBudget;
  'migration-start': DirectScenarioPhaseBudget;
  'migration-interrupt': DirectScenarioPhaseBudget;
  'migration-restart': DirectScenarioPhaseBudget;
  migration: DirectScenarioPhaseBudget;
  'post-migration': DirectScenarioPhaseBudget;
  'inventory-after': DirectScenarioPhaseBudget;
  'audit-after': DirectScenarioPhaseBudget;
  'failed-recovery': DirectScenarioPhaseBudget;
  'cleanup-recovery': DirectScenarioPhaseBudget;
  'provision-recovery': DirectScenarioPhaseBudget;
  'delete-objects': DirectScenarioPhaseBudget;
  'decommission-a': DirectScenarioPhaseBudget;
  'decommission-b': DirectScenarioPhaseBudget;
  'force-recovery': DirectScenarioPhaseBudget;
  'force-observe': DirectScenarioPhaseBudget;
  'recover-force-residual': DirectScenarioPhaseBudget;
  complete: DirectScenarioPhaseBudget;
}>;

export const DIRECT_SCENARIO_PHASES: readonly (keyof typeof DIRECT_SCENARIO_INVOCATION_BUDGET)[];

export const DIRECT_SCENARIO_MIN_INVOCATIONS: number;
