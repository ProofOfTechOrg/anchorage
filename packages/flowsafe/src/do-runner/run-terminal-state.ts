// SPDX-License-Identifier: Apache-2.0

import type { UpdateWorkflowStateOptions } from '@mastra/core/storage';
import type { WorkflowRunStatus, WorkflowState } from '@mastra/core/workflows';
import type { RunTerminalStatus } from './run-lifecycle.js';

export type RunStatus =
  | WorkflowRunStatus
  | 'waiting_callback'
  | 'waiting_signal'
  | 'retry_wait'
  | RunTerminalStatus;

export type CoreRunResult =
  | { status: 'success'; result: unknown }
  | { status: 'failed'; error: unknown }
  | {
      status: 'suspended';
      suspended: [string[], ...string[][]];
      suspendPayload?: unknown;
      steps?: WorkflowState['steps'];
    }
  | { status: 'tripwire'; tripwire: { reason: string } }
  | {
      status: Exclude<
        WorkflowRunStatus,
        'success' | 'failed' | 'suspended' | 'tripwire'
      >;
    };

const RUN_STATUSES = {
  running: true,
  success: true,
  failed: true,
  tripwire: true,
  suspended: true,
  waiting: true,
  pending: true,
  canceled: true,
  bailed: true,
  paused: true,
  skipped: true,
  waiting_callback: true,
  waiting_signal: true,
  retry_wait: true,
  cancelled: true,
  timed_out: true,
} satisfies Record<RunStatus, true>;

export function isRunStatus(value: unknown): value is RunStatus {
  return typeof value === 'string' && Object.hasOwn(RUN_STATUSES, value);
}

export function isTerminalRunStatus(value: unknown): boolean {
  return (
    value === 'success' ||
    value === 'failed' ||
    value === 'tripwire' ||
    value === 'canceled' ||
    value === 'bailed' ||
    value === 'skipped' ||
    value === 'cancelled' ||
    value === 'timed_out'
  );
}

const NONTERMINAL_RUN_STATUSES = new Set<WorkflowRunStatus>([
  'running',
  'suspended',
  'waiting',
  'pending',
  'paused',
]);

export function terminalStateFields(
  status: RunStatus,
): UpdateWorkflowStateOptions {
  return {
    status: status as WorkflowRunStatus,
    result: undefined,
    error: undefined,
    suspendedPaths: {},
    waitingPaths: {},
    resumeLabels: {},
    activePaths: [],
    activeStepsPath: {},
  };
}

export function terminalStateUpdate(
  result: CoreRunResult,
): UpdateWorkflowStateOptions | undefined {
  if (NONTERMINAL_RUN_STATUSES.has(result.status)) return undefined;
  const common = terminalStateFields(result.status);
  if (result.status === 'success') {
    return {
      ...common,
      result: result.result as UpdateWorkflowStateOptions['result'],
    };
  }
  if (result.status === 'failed') {
    const error = result.error;
    const name =
      error instanceof Error
        ? error.name
        : error !== null &&
            typeof error === 'object' &&
            'name' in error &&
            typeof (error as { name: unknown }).name === 'string'
          ? (error as { name: string }).name
          : 'Error';
    const stack =
      error instanceof Error
        ? error.stack
        : error !== null &&
            typeof error === 'object' &&
            'stack' in error &&
            typeof (error as { stack: unknown }).stack === 'string'
          ? (error as { stack: string }).stack
          : undefined;
    return {
      ...common,
      error: {
        name,
        message: errorText(error),
        ...(stack !== undefined ? { stack } : {}),
      },
    };
  }
  return common;
}

// Persistence may serialize an Error into a plain object.
export function errorText(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  if (
    error !== null &&
    typeof error === 'object' &&
    'message' in error &&
    typeof (error as { message: unknown }).message === 'string'
  ) {
    return (error as { message: string }).message;
  }
  return String(error);
}
