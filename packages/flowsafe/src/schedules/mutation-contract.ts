// SPDX-License-Identifier: Apache-2.0

import type { Schedule, ScheduleUpdate } from '@mastra/core/storage';
import type { ResourceOwner } from '../approval-api/resource-ownership.js';
import { DoStatusError } from '../do-runner/do-status-error.js';
import type { MutationEpochContext } from '../do-runner/execution-admission.js';
import type { SignalDatabase } from '../signals/d1-shared.js';
import type { AuthorizedSchedule } from './target-policy.js';

export const FENCED_SCHEDULE_STORAGE: unique symbol = Symbol(
  'flowsafe.fencedScheduleStorage',
);

export interface ScheduleResumeMutation {
  readonly expectedCron: Schedule['cron'];
  readonly expectedTimezone: Schedule['timezone'];
  readonly nextFireAt: Schedule['nextFireAt'];
}

/** Epoch activation can race a waiting request, so this contract applies before activation. */
export interface FencedScheduleMutationCapability {
  readonly database: SignalDatabase & Required<Pick<SignalDatabase, 'batch'>>;
  createOwnedSchedule(
    schedule: AuthorizedSchedule,
    owner: ResourceOwner,
    maxSchedules: number,
    context: MutationEpochContext,
  ): Promise<Schedule | null>;
  updateSchedule(
    id: string,
    patch: ScheduleUpdate,
    context: MutationEpochContext,
  ): Promise<Schedule>;
  pauseSchedule(id: string, context: MutationEpochContext): Promise<Schedule>;
  resumeSchedule(
    id: string,
    mutation: ScheduleResumeMutation,
    context: MutationEpochContext,
  ): Promise<Schedule>;
  deleteOwnedSchedule(
    id: string,
    context: MutationEpochContext,
  ): Promise<'deleted' | 'pending'>;
  observeScheduleMutation(
    id: string,
    operation: 'pause' | 'resume',
    context: MutationEpochContext,
  ): Promise<Schedule | null>;
}

export class ScheduleMutationConflictError extends DoStatusError {
  readonly status = 409;
  readonly reason: {
    readonly code: 'SCHEDULE_MUTATION_CONFLICT';
    readonly classification: 'fence-changed' | 'schedule-changed';
  };

  constructor(classification: 'fence-changed' | 'schedule-changed') {
    super('schedule mutation conflicted with a concurrent change');
    this.name = 'ScheduleMutationConflictError';
    this.reason = { code: 'SCHEDULE_MUTATION_CONFLICT', classification };
  }
}

export class ScheduleMutationOutcomeUnknownError extends DoStatusError {
  readonly status = 503;
  readonly reason = { code: 'SCHEDULE_MUTATION_OUTCOME_UNKNOWN' } as const;

  constructor(options?: ErrorOptions) {
    super('schedule mutation outcome is unknown', options);
    this.name = 'ScheduleMutationOutcomeUnknownError';
  }
}
