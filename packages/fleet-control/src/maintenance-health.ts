// SPDX-License-Identifier: Apache-2.0

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
    throw new Error(`maintenance request failed with HTTP ${response.status}`);
  }
  const body: unknown = await response.json();
  if (!body || typeof body !== 'object') {
    throw new Error('maintenance response must be a JSON object');
  }
  const value = body as Record<string, unknown>;
  const nextAlarmAt = timestamp(value.alarmAt, 'alarmAt');
  const tickConfigured =
    value.nextTickAt !== undefined ||
    value.lastTickAt !== undefined ||
    value.lastTickAttemptAt !== undefined ||
    value.lastTickError !== undefined;
  const lastSweepError = errorMessage(value.lastSweepError, 'lastSweepError');
  const lastPurgeError = errorMessage(value.lastPurgeError, 'lastPurgeError');
  const lastTickError = errorMessage(value.lastTickError, 'lastTickError');
  const specDigest = deploymentSpecDigest(value.deploymentSpecDigest);
  return {
    armed: nextAlarmAt !== null,
    nextAlarmAt,
    ...(specDigest === undefined ? {} : { deploymentSpecDigest: specDigest }),
    lastSweepAt: timestamp(value.lastSweepAt, 'lastSweepAt'),
    lastPurgeAt: timestamp(value.lastPurgeAt, 'lastPurgeAt'),
    ...(value.lastSweepAttemptAt === undefined
      ? {}
      : {
          lastSweepAttemptAt: timestamp(
            value.lastSweepAttemptAt,
            'lastSweepAttemptAt',
          ),
        }),
    ...(value.lastPurgeAttemptAt === undefined
      ? {}
      : {
          lastPurgeAttemptAt: timestamp(
            value.lastPurgeAttemptAt,
            'lastPurgeAttemptAt',
          ),
        }),
    ...(lastSweepError === undefined ? {} : { lastSweepError }),
    ...(lastPurgeError === undefined ? {} : { lastPurgeError }),
    ...(tickConfigured
      ? {
          lastTickAt: timestamp(value.lastTickAt, 'lastTickAt'),
          ...(value.lastTickAttemptAt === undefined
            ? {}
            : {
                lastTickAttemptAt: timestamp(
                  value.lastTickAttemptAt,
                  'lastTickAttemptAt',
                ),
              }),
          ...(lastTickError === undefined ? {} : { lastTickError }),
        }
      : {}),
  };
}
