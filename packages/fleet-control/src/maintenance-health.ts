// SPDX-License-Identifier: Apache-2.0

import { cancelBodyWithoutAwait } from './database-export-store.js';
import { isSha256 } from './deployment-context.js';
import type { DeploymentSpec, MaintenanceHealth } from './types.js';

export function maintenanceUrl(spec: DeploymentSpec, path: string): URL {
  return new URL(
    path,
    spec.maintenanceBaseUrl.endsWith('/')
      ? spec.maintenanceBaseUrl
      : `${spec.maintenanceBaseUrl}/`,
  );
}

function timestamp(value: unknown, field: string): number | null {
  if (value === null || value === undefined) return null;
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new Error(`maintenance response field '${field}' is invalid`);
  }
  return value as number;
}

function errorMessage(value: unknown, field: string): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'string' || value.length === 0 || value.length > 1_024) {
    throw new Error(`maintenance response field '${field}' is invalid`);
  }
  return value;
}

/**
 * The duties the fleet audit watches, with the FlowSafe maintenance status
 * fields of each. An `always` duty is watched from every status. Any other
 * duty is watched only when the status carries one of its fields: a
 * deployment that does not run it reports none, and so does FlowSafe before
 * 0.16.0 for the run-deadline duty, which would otherwise read as a duty that
 * never succeeded.
 */
export const MAINTENANCE_DUTIES = [
  {
    name: 'sweep',
    always: true,
    next: 'nextSweepAt',
    last: 'lastSweepAt',
    attempt: 'lastSweepAttemptAt',
    error: 'lastSweepError',
  },
  {
    name: 'purge',
    always: true,
    next: 'nextPurgeAt',
    last: 'lastPurgeAt',
    attempt: 'lastPurgeAttemptAt',
    error: 'lastPurgeError',
  },
  {
    name: 'deadline',
    always: false,
    next: 'nextDeadlineAt',
    last: 'lastDeadlineAt',
    attempt: 'lastDeadlineAttemptAt',
    error: 'lastDeadlineError',
  },
  {
    name: 'tick',
    always: false,
    next: 'nextTickAt',
    last: 'lastTickAt',
    attempt: 'lastTickAttemptAt',
    error: 'lastTickError',
  },
  {
    name: 'notification',
    always: false,
    next: 'nextNotificationAt',
    last: 'lastNotificationAt',
    attempt: 'lastNotificationAttemptAt',
    error: 'lastNotificationError',
  },
] as const satisfies readonly {
  readonly name: string;
  readonly always: boolean;
  readonly next: string;
  readonly last: keyof MaintenanceHealth;
  readonly attempt: keyof MaintenanceHealth;
  readonly error: keyof MaintenanceHealth;
}[];

export type MaintenanceDutyName = (typeof MAINTENANCE_DUTIES)[number]['name'];

type MaintenanceDutyField = (typeof MAINTENANCE_DUTIES)[number][
  | 'last'
  | 'attempt'
  | 'error'];

type MaintenanceDutyHealth = {
  -readonly [K in MaintenanceDutyField]?: MaintenanceHealth[K];
};

function deploymentSpecDigest(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'string' || !isSha256(value)) {
    throw new Error(
      "maintenance response field 'deploymentSpecDigest' is invalid",
    );
  }
  return value;
}

export async function readMaintenanceHealth(
  response: Response,
): Promise<MaintenanceHealth> {
  if (!response.ok) {
    const refusal = new Error(
      `maintenance request failed with HTTP ${response.status}`,
    );
    cancelBodyWithoutAwait(response.body, refusal);
    throw refusal;
  }
  const body: unknown = await response.json();
  if (!body || typeof body !== 'object') {
    throw new Error('maintenance response must be a JSON object');
  }
  const value = body as Record<string, unknown>;
  const nextAlarmAt = timestamp(value.alarmAt, 'alarmAt');
  const duties: MaintenanceDutyHealth = {};
  for (const duty of MAINTENANCE_DUTIES) {
    const reported = [duty.next, duty.last, duty.attempt, duty.error].some(
      (field) => value[field] !== undefined,
    );
    if (!duty.always && !reported) continue;
    duties[duty.last] = timestamp(value[duty.last], duty.last);
    if (value[duty.attempt] !== undefined) {
      duties[duty.attempt] = timestamp(value[duty.attempt], duty.attempt);
    }
    const error = errorMessage(value[duty.error], duty.error);
    if (error !== undefined) duties[duty.error] = error;
  }
  const specDigest = deploymentSpecDigest(value.deploymentSpecDigest);
  return {
    armed: nextAlarmAt !== null,
    nextAlarmAt,
    ...(specDigest === undefined ? {} : { deploymentSpecDigest: specDigest }),
    ...duties,
    lastSweepAt: duties.lastSweepAt ?? null,
    lastPurgeAt: duties.lastPurgeAt ?? null,
  };
}
