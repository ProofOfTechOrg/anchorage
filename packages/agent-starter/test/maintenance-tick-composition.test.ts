// SPDX-License-Identifier: Apache-2.0
// The maintenance tick against mocked leg factories: the notification tick is
// constructed once, at wiring time, and each pass invokes both legs and returns
// both results. The mocks are file-scoped, so these cases live apart from the
// rest of the starter's tick coverage.

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

// Hoisted with the `vi.mock` factories that reference them: a factory runs
// above module-scope bindings, so one closing over a plain `const` throws
// `Cannot access '<name>' before initialization` at import.
const mocks = vi.hoisted(() => {
  const scheduleResult: ScheduleTickResult = {
    due: 0,
    fired: 0,
    skipped: 0,
    failed: 0,
    deferred: 0,
    reconciled: 0,
    lost: 0,
  };
  const notificationResult: NotificationDispatchTickResult = {
    due: 2,
    delivered: 1,
    failed: 1,
  };
  const scheduleTick = vi.fn(
    async (): Promise<ScheduleTickResult> => ({ ...scheduleResult }),
  );
  const notificationTick = vi.fn(
    async (): Promise<NotificationDispatchTickResult> => ({
      ...notificationResult,
    }),
  );
  return {
    scheduleResult,
    notificationResult,
    scheduleTick,
    notificationTick,
    createScheduleTick: vi.fn(
      (_options: ScheduleTickOptions): (() => Promise<ScheduleTickResult>) =>
        scheduleTick,
    ),
    createNotificationDispatchTick: vi.fn(
      (
        _options: NotificationDispatchTickOptions,
      ): (() => Promise<NotificationDispatchTickResult>) => notificationTick,
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
      'the maintenance tick addressed a Durable Object; no pass in this file should',
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

beforeEach(() => {
  mocks.scheduleTick.mockClear();
  mocks.createScheduleTick.mockClear();
  mocks.createNotificationDispatchTick.mockClear();
  mocks.notificationTick.mockClear();
});

describe('starter maintenance tick composition', () => {
  it('constructs the notification tick at wiring time and invokes it per pass', async () => {
    const tick = starterMaintenanceTick(starterEnv());

    // Wiring builds the tick; it does not run it.
    expect(mocks.createNotificationDispatchTick).toHaveBeenCalledTimes(1);
    expect(mocks.notificationTick).not.toHaveBeenCalled();

    const first = await tick();
    const second = await tick();

    // Both legs' results reach the caller, so a leg that returned its
    // constructed tick instead of calling it goes red here.
    const composed = {
      schedules: mocks.scheduleResult,
      notifications: mocks.notificationResult,
    };
    expect(first).toEqual(composed);
    expect(second).toEqual(composed);
    expect(mocks.scheduleTick).toHaveBeenCalledTimes(2);
    expect(mocks.notificationTick).toHaveBeenCalledTimes(2);
    expect(mocks.createNotificationDispatchTick).toHaveBeenCalledTimes(1);
  });
});
