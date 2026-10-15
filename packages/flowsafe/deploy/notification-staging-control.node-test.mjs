import assert from 'node:assert/strict';
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import {
  localProbe,
  readSecretFile,
  runNotificationStagingControl,
  trustedD1Checks,
} from './notification-staging-control.mjs';

function fixture(failAt, initialState = 'migration-locked') {
  const calls = [];
  let fence = {
    state: initialState,
    mutationEpoch: 0,
    requireMutationEpoch: false,
    transitionRevision: 0,
  };
  const probe = async (method, path, body) => {
    calls.push({ method, path, body });
    if (method === 'GET' && path === '/admin/execution-fence')
      return { ...fence };
    if (method === 'GET' && path === '/admin/maintenance-status') {
      return { alarmAt: 1, nextRemoteStepRecoveryAt: 1 };
    }
    if (path === '/admin/execution-fence') {
      assert.equal(body.expected, fence.state);
      assert.equal(body.expectedMutationEpoch, fence.mutationEpoch);
      assert.equal(body.expectedRevision, fence.transitionRevision);
      fence = {
        state: body.next,
        mutationEpoch: fence.mutationEpoch + 1,
        requireMutationEpoch: true,
        transitionRevision: fence.transitionRevision + 1,
      };
      if (failAt === 'unlock-response' && body.next === 'open')
        throw new Error('response lost after accepted unlock');
      return { ...fence };
    }
    if (path === '/start') {
      if (failAt === 'start') throw new Error('start failed');
      return { status: 'suspended', invocationId: 'invocation-1' };
    }
    if (path === '/complete') {
      if (failAt === 'complete') throw new Error('complete failed');
      return { status: 'success', invocationId: body.invocationId };
    }
    throw new Error('unexpected request');
  };
  let offRequests = 0;
  const options = {
    probe,
    preflightD1: async () => {
      if (failAt === 'preflight') throw new Error('preflight failed');
    },
    verifyD1: async () => {
      if (failAt === 'verify') throw new Error('verification failed');
    },
    requestOffDeploy: async () => {
      offRequests++;
    },
  };
  return {
    options,
    calls,
    fence: () => fence,
    offRequests: () => offRequests,
  };
}

test('preflight mismatch and read-only failure never unlock', async () => {
  for (const scenario of [fixture(null, 'open'), fixture('preflight')]) {
    await assert.rejects(runNotificationStagingControl(scenario.options));
    assert.equal(
      scenario.calls.filter((call) => call.method === 'POST').length,
      0,
    );
    assert.equal(scenario.offRequests(), 1);
  }
});

test('each post-unlock failure requests a fresh CAS relock and off deploy', async () => {
  for (const step of ['unlock-response', 'start', 'complete', 'verify']) {
    const scenario = fixture(step);
    await assert.rejects(runNotificationStagingControl(scenario.options));
    assert.deepEqual(scenario.fence(), {
      state: 'migration-locked',
      mutationEpoch: 2,
      requireMutationEpoch: true,
      transitionRevision: 2,
    });
    assert.equal(scenario.offRequests(), 1);
    const writes = scenario.calls.filter(
      (call) =>
        call.method === 'POST' && call.path === '/admin/execution-fence',
    );
    assert.deepEqual(
      writes.map((call) => call.body.next),
      ['open', 'migration-locked'],
    );
    assert.deepEqual(
      [writes[0].body.expectedRevision, writes[1].body.expectedRevision],
      [0, 1],
    );
  }
});

test('a successful proof verifies before relock and requests off deploy', async () => {
  const scenario = fixture();
  assert.deepEqual(await runNotificationStagingControl(scenario.options), {
    invocationId: 'invocation-1',
    verdict: 'notification_staging_passed',
  });
  assert.equal(scenario.fence().state, 'migration-locked');
  assert.equal(scenario.offRequests(), 1);
});

test('a conflicting fence is not overwritten while off deploy is still requested', async () => {
  const scenario = fixture();
  scenario.options.verifyD1 = async () => {
    throw new Error('verification failed');
  };
  const original = scenario.options.probe;
  scenario.options.probe = async (method, path, body) => {
    if (
      method === 'GET' &&
      path === '/admin/execution-fence' &&
      scenario.calls.some((call) => call.path === '/start')
    ) {
      return {
        state: 'draining',
        mutationEpoch: 1,
        requireMutationEpoch: true,
        transitionRevision: 2,
      };
    }
    return original(method, path, body);
  };
  await assert.rejects(
    runNotificationStagingControl(scenario.options),
    /relock is unconfirmed/,
  );
  assert.equal(scenario.fence().state, 'open');
  assert.equal(scenario.offRequests(), 1);
});

test('probe reads a private secret file and calls only loopback', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'flowsafe-probe-secret-test-'));
  try {
    const path = join(directory, 'secret');
    const secret = 'a'.repeat(32);
    writeFileSync(path, secret, { mode: 0o600 });
    const requests = [];
    const probe = localProbe({
      port: 8897,
      secret: readSecretFile(path),
      fetcher: async (url, init) => {
        requests.push({ url, init });
        return Response.json({ state: 'migration-locked' });
      },
    });
    assert.deepEqual(await probe('GET', '/admin/execution-fence'), {
      state: 'migration-locked',
    });
    assert.equal(
      requests[0].url,
      'http://127.0.0.1:8897/admin/execution-fence',
    );
    assert.equal(requests[0].init.headers.authorization, `Bearer ${secret}`);
    chmodSync(path, 0o644);
    assert.throws(() => readSecretFile(path), /private/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test('D1 proof requires the receipt tuple and one new notification and receipt', async () => {
  const db = new DatabaseSync(':memory:');
  try {
    db.exec(`CREATE TABLE flowsafe_remote_step_invocations (
      invocation_id TEXT PRIMARY KEY, status TEXT, settlement_outcome TEXT,
      operation_state TEXT, thread_id TEXT, notification_id TEXT,
      operation_key TEXT, notification_request_digest TEXT,
      write_generation INTEGER NOT NULL
    );
    CREATE TABLE flowsafe_notification_operation_receipts (
      operation_key TEXT PRIMARY KEY, request_digest TEXT,
      thread_id TEXT, notification_id TEXT
    );
    CREATE TABLE mastra_notifications (
      id TEXT, thread_id TEXT, source TEXT, kind TEXT
    );`);
    const checks = trustedD1Checks((sql) => db.prepare(sql).all());
    await checks.preflightD1();
    db.exec(`INSERT INTO flowsafe_remote_step_invocations VALUES
      ('invocation-1', 'settled', 'applied', 'resume-admitted',
       'thread-1', 'notification-1', 'operation-1', '${'a'.repeat(64)}', 1);
      INSERT INTO mastra_notifications VALUES
      ('notification-1', 'thread-1', 'flowsafe-staging', 'host-owned-notification');
      INSERT INTO flowsafe_notification_operation_receipts VALUES
      ('operation-1', '${'a'.repeat(64)}', 'other-thread', 'notification-1');`);
    await assert.rejects(checks.verifyD1('invocation-1'), /unconfirmed/);
    db.exec(
      "UPDATE flowsafe_notification_operation_receipts SET thread_id = 'thread-1', notification_id = 'other-notification'",
    );
    await assert.rejects(checks.verifyD1('invocation-1'), /unconfirmed/);
    db.exec(
      "UPDATE flowsafe_notification_operation_receipts SET notification_id = 'notification-1'",
    );
    await checks.verifyD1('invocation-1');
    db.exec(
      `UPDATE flowsafe_notification_operation_receipts SET request_digest = '${'b'.repeat(64)}'`,
    );
    await assert.rejects(checks.verifyD1('invocation-1'), /unconfirmed/);
    db.exec(
      `UPDATE flowsafe_notification_operation_receipts SET request_digest = '${'a'.repeat(64)}'`,
    );
    db.exec(`INSERT INTO mastra_notifications VALUES
      ('notification-2', 'thread-2', 'flowsafe-staging', 'host-owned-notification');`);
    await assert.rejects(checks.verifyD1('invocation-1'), /count differs/);
  } finally {
    db.close();
  }
});

test('D1 preflight accepts an absent receipt table but proof requires its effect', async () => {
  const db = new DatabaseSync(':memory:');
  try {
    db.exec(`CREATE TABLE flowsafe_remote_step_invocations (
      invocation_id TEXT PRIMARY KEY, status TEXT, settlement_outcome TEXT,
      operation_state TEXT, thread_id TEXT, notification_id TEXT,
      operation_key TEXT, notification_request_digest TEXT,
      write_generation INTEGER NOT NULL
    );
    CREATE TABLE mastra_notifications (
      id TEXT, thread_id TEXT, source TEXT, kind TEXT
    );`);
    const checks = trustedD1Checks((sql) => db.prepare(sql).all());
    await checks.preflightD1();
    db.exec(`INSERT INTO flowsafe_remote_step_invocations VALUES
      ('invocation-1', 'settled', 'applied', 'resume-admitted',
       'thread-1', 'notification-1', 'operation-1', '${'a'.repeat(64)}', 1);
      INSERT INTO mastra_notifications VALUES
      ('notification-1', 'thread-1', 'flowsafe-staging', 'host-owned-notification');`);
    await assert.rejects(checks.verifyD1('invocation-1'), /no such table/);
    db.exec(`CREATE TABLE flowsafe_notification_operation_receipts (
      operation_key TEXT PRIMARY KEY, request_digest TEXT,
      thread_id TEXT, notification_id TEXT
    );
    INSERT INTO flowsafe_notification_operation_receipts VALUES
      ('operation-1', '${'a'.repeat(64)}', 'thread-1', 'notification-1');`);
    await checks.verifyD1('invocation-1');
  } finally {
    db.close();
  }
});
