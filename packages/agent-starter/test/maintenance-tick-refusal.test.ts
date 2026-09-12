// SPDX-License-Identifier: Apache-2.0
// The maintenance tick against an @mastra/core that refuses notification
// dispatch construction. The mocks are file-scoped, so these cases live apart
// from the rest of the starter's tick coverage.

import type {
  ScheduleTickOptions,
  ScheduleTickResult,
} from '@proofoftech/flowsafe/schedules';
import type {
  NotificationDispatchTickOptions,
  NotificationDispatchTickResult,
} from '@proofoftech/flowsafe/signals';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { starterMaintenanceTick } from '../src/maintenance.js';

const PATCH_MESSAGE = /Apply the flowsafe patch to @mastra\/core/;

// Hoisted with the `vi.mock` factories that reference them: a factory runs
// above module-scope bindings, so one closing over a plain `const` throws
// `Cannot access '<name>' before initialization` at import.
const mocks = vi.hoisted(() => {
  const scheduleTick = vi.fn(
    async (): Promise<ScheduleTickResult> => ({
      due: 0,
      fired: 0,
      skipped: 0,
      failed: 0,
      deferred: 0,
      reconciled: 0,
      lost: 0,
    }),
  );
  return {
    scheduleTick,
    createScheduleTick: vi.fn(
      (_options: ScheduleTickOptions): (() => Promise<ScheduleTickResult>) =>
        scheduleTick,
    ),
    createNotificationDispatchTick: vi.fn(
      (
        _options: NotificationDispatchTickOptions,
      ): (() => Promise<NotificationDispatchTickResult>) => {
        throw new TypeError(
          'notification dispatch requires the @mastra/core patch flowsafe ships; apply it at the application root (getting started: "Apply the flowsafe patch to @mastra/core")',
        );
      },
    ),
  };
});

// The spread keeps every other export live: storage.ts imports the D1 storage
// classes from these same two barrels.
vi.mock('@proofoftech/flowsafe/schedules', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@proofoftech/flowsafe/schedules')>()),
  createScheduleTick: mocks.createScheduleTick,
}));

vi.mock('@proofoftech/flowsafe/signals', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@proofoftech/flowsafe/signals')>()),
  createNotificationDispatchTick: mocks.createNotificationDispatchTick,
}));

/**
 * A namespace stub. The tick builds its run/thread topologies eagerly, and no
 * pass here addresses one — reaching this is the failure.
 */
function namespace(): Env['RUNNER'] {
  const unreachable = () => {
    throw new Error(
      'a fenced maintenance pass addressed a Durable Object — it claimed work',
    );
  };
  return {
    idFromName: unreachable,
    idFromString: unreachable,
    newUniqueId: unreachable,
    get: unreachable,
  } as unknown as Env['RUNNER'];
}

/**
 * A plain object stands in for the D1 binding: both tick factories are mocked,
 * and the wiring-time store calls only key a `WeakMap` on the binding, so no
 * statement here reads a database.
 */
function starterEnv(): Env {
  return {
    DB: {} as Env['DB'],
    DEPLOYMENT_TENANT: 'acme',
    DEPLOYMENT_IDENTITY_SECRET: 'test-deployment-identity-secret-0001',
    RUNNER: namespace(),
    THREAD: namespace(),
  } as unknown as Env;
}

describe('starter maintenance tick against an unpatched core', () => {
  beforeEach(() => {
    mocks.scheduleTick.mockClear();
    mocks.createScheduleTick.mockClear();
    mocks.createNotificationDispatchTick.mockClear();
  });

  it('wires the duty without constructing the notification tick', () => {
    expect(() => starterMaintenanceTick(starterEnv())).not.toThrow();
    expect(mocks.createNotificationDispatchTick).not.toHaveBeenCalled();
  });

  it('fails the notifications leg after the schedule leg has run', async () => {
    const tick = starterMaintenanceTick(starterEnv());

    await expect(tick()).rejects.toThrow(PATCH_MESSAGE);

    expect(mocks.scheduleTick).toHaveBeenCalledTimes(1);
    expect(mocks.createNotificationDispatchTick).toHaveBeenCalledTimes(1);
  });

  it('retries the refused construction on the next pass', async () => {
    const tick = starterMaintenanceTick(starterEnv());

    await expect(tick()).rejects.toThrow(PATCH_MESSAGE);
    await expect(tick()).rejects.toThrow(PATCH_MESSAGE);

    expect(mocks.scheduleTick).toHaveBeenCalledTimes(2);
    expect(mocks.createNotificationDispatchTick).toHaveBeenCalledTimes(2);
  });
});
