import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import {
  createWranglerControl,
  sealFence,
} from './notification-staging-live.mjs';

const enabled = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa';
const disabled = 'bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb';

function fakeCli({ enabledFlag = true } = {}) {
  const calls = [];
  let version = enabled;
  const command = (_executable, args) => {
    calls.push(args);
    if (args[0] === 'deployments') {
      return {
        status: 0,
        stdout: JSON.stringify([
          {
            created_on: '2026-10-15T00:00:00.000Z',
            versions: [{ version_id: version, percentage: 100 }],
          },
        ]),
      };
    }
    if (args[0] === 'versions') {
      return {
        status: 0,
        stdout: JSON.stringify({
          resources: {
            bindings: [
              {
                name: 'NOTIFICATION_PROOF_ENABLED',
                type: 'plain_text',
                text: version === enabled && enabledFlag ? 'true' : 'false',
              },
            ],
          },
        }),
      };
    }
    if (args[0] === 'deploy') {
      version = disabled;
      return { status: 0, stdout: 'deployed' };
    }
    if (args[0] === 'd1') {
      return {
        status: 0,
        stdout: JSON.stringify([{ success: true, results: [{ count: 0 }] }]),
      };
    }
    throw new Error('unexpected CLI');
  };
  const cli = createWranglerControl({ command, token: 'fake-token' });
  return {
    cli,
    calls,
    change() {
      version = disabled;
    },
  };
}

test('version-checked off deploy uses the tracked false config', () => {
  const fixture = fakeCli();
  assert.equal(fixture.cli.currentVersion(), enabled);
  assert.equal(fixture.cli.proofEnabled(enabled), true);
  assert.equal(
    fixture.cli.deployOffIfCurrent(enabled, '/tmp/staging-off-test'),
    disabled,
  );
  assert.equal(fixture.cli.proofEnabled(disabled), false);
  assert.equal(fixture.calls.filter((args) => args[0] === 'deploy').length, 1);
});

test('changed version or disabled flag refuses before deploy', () => {
  const changed = fakeCli();
  changed.change();
  assert.throws(
    () => changed.cli.deployOffIfCurrent(enabled, '/tmp/staging-off-test'),
    /changed/,
  );
  assert.equal(changed.calls.filter((args) => args[0] === 'deploy').length, 0);
  const disabledFlag = fakeCli({ enabledFlag: false });
  assert.throws(
    () => disabledFlag.cli.deployOffIfCurrent(enabled, '/tmp/staging-off-test'),
    /not enabled/,
  );
  assert.equal(
    disabledFlag.calls.filter((args) => args[0] === 'deploy').length,
    0,
  );
});

test('Wrangler failure preserves exit and stderr in its diagnostic cause', () => {
  const cli = createWranglerControl({
    token: 'fake-token',
    command: () => ({ status: 1, signal: null, stderr: 'deployment rejected' }),
  });
  assert.throws(
    () => cli.currentVersion(),
    (error) => {
      assert.match(error.message, /Wrangler deployments list failed/);
      assert.match(error.cause?.message, /exit=1/);
      assert.match(error.cause?.message, /deployment rejected/);
      return true;
    },
  );
});

test('watchdog seals the initial fence against a delayed controller open', async () => {
  const calls = [];
  let fence = {
    state: 'migration-locked',
    mutationEpoch: 0,
    transitionRevision: 0,
    requireMutationEpoch: false,
  };
  const probe = async (method, path, body) => {
    calls.push({ method, path, body });
    if (method === 'GET') return fence;
    assert.equal(body.expected, 'migration-locked');
    assert.equal(body.next, 'migration-locked');
    assert.equal(body.expectedMutationEpoch, 0);
    assert.equal(body.expectedRevision, 0);
    fence = {
      state: 'migration-locked',
      mutationEpoch: 1,
      transitionRevision: 1,
      requireMutationEpoch: true,
    };
    return fence;
  };
  assert.deepEqual(await sealFence(probe), fence);
  assert.equal(calls.filter(({ method }) => method === 'POST').length, 1);
  assert.equal(fence.mutationEpoch, 1);
  assert.equal(fence.transitionRevision, 1);
});

test('watchdog relocks when controller open wins the deadline CAS', async () => {
  let fence = {
    state: 'migration-locked',
    mutationEpoch: 0,
    transitionRevision: 0,
    requireMutationEpoch: false,
  };
  const probe = async (method, _path, body) => {
    if (method === 'GET') return fence;
    if (body.expected === 'migration-locked') {
      fence = {
        state: 'open',
        mutationEpoch: 1,
        transitionRevision: 1,
        requireMutationEpoch: true,
      };
      throw new Error('controller open won');
    }
    assert.deepEqual(body, {
      expected: 'open',
      next: 'migration-locked',
      expectedMutationEpoch: 1,
      expectedRevision: 1,
      advanceMutationEpoch: true,
    });
    fence = {
      state: 'migration-locked',
      mutationEpoch: 2,
      transitionRevision: 2,
      requireMutationEpoch: true,
    };
    return fence;
  };
  assert.deepEqual(await sealFence(probe), fence);
  assert.equal(fence.state, 'migration-locked');
  assert.equal(fence.transitionRevision, 2);
});

test('watchdog resolves a lost relock response from the authoritative fence', async () => {
  let fence = {
    state: 'open',
    mutationEpoch: 1,
    transitionRevision: 1,
    requireMutationEpoch: true,
  };
  const probe = async (method, _path, body) => {
    if (method === 'GET') return fence;
    assert.equal(body.expected, 'open');
    fence = {
      state: 'migration-locked',
      mutationEpoch: 2,
      transitionRevision: 2,
      requireMutationEpoch: true,
    };
    throw new Error('response lost after commit');
  };
  assert.deepEqual(await sealFence(probe), fence);
});

async function exerciseWatchdog(outcome) {
  const directory = mkdtempSync(join(tmpdir(), 'flowsafe-live-watchdog-test-'));
  const secret = 's'.repeat(32);
  const tokenFile = join(directory, 'cf-token');
  const secretFile = join(directory, 'probe-secret');
  const preloadFile = join(directory, 'fake-wrangler.mjs');
  const offFile = join(directory, 'off-deploy');
  const offConfirmedFile = join(directory, 'off-confirmed');
  const watcherPidFile = join(directory, 'watchdog-pid');
  const deployStartedFile = join(directory, 'deploy-started');
  const releaseDeployFile = join(directory, 'release-deploy');
  const accountId =
    outcome === 'parent death' ? 'c'.repeat(32) : 'd'.repeat(32);
  const lockFile = join(
    '/tmp',
    `flowsafe-deploy-locks-${process.getuid()}`,
    `${accountId}-anchorage-flowsafe-remote-step-staging.lock`,
  );
  writeFileSync(tokenFile, 'fake-token', { mode: 0o600 });
  writeFileSync(secretFile, secret, { mode: 0o600 });
  writeFileSync(
    preloadFile,
    `import childProcess from 'node:child_process';
import { syncBuiltinESMExports } from 'node:module';
import { existsSync, writeFileSync } from 'node:fs';
const originalSpawn = childProcess.spawn;
const originalSpawnSync = childProcess.spawnSync;
const originalNow = Date.now;
Date.now = () => originalNow() +
  (process.env.TEST_CLOCK_JUMP === 'true' &&
   !process.argv.includes('--watchdog') &&
   existsSync(process.env.TEST_DEPLOY_STARTED_FILE) ? 300_000 : 0);
let counts = 0;
childProcess.spawn = (...args) => {
  const child = originalSpawn(...args);
  if (args[1]?.includes('--watchdog'))
    writeFileSync(process.env.TEST_WATCHDOG_PID_FILE, String(child.pid));
  return child;
};
childProcess.spawnSync = (executable, args, options) => {
  if (executable === 'flock') return originalSpawnSync(executable, args, options);
  if (!executable.endsWith('/node_modules/.bin/wrangler'))
    throw new Error('unexpected synchronous command');
  const off = existsSync(process.env.TEST_OFF_FILE);
  const command = args[0];
  if (off && command === 'deployments')
    writeFileSync(process.env.TEST_OFF_CONFIRMED_FILE, 'checked');
  const response = command === 'deployments' ? [{
    created_on: '2026-10-15T00:00:00.000Z',
    versions: [{ version_id: off ? '${disabled}' : '${enabled}', percentage: 100 }],
  }] : command === 'versions' ? {
    resources: { bindings: [{
      name: 'NOTIFICATION_PROOF_ENABLED', type: 'plain_text',
      text: off ? 'false' : 'true',
    }] },
  } : command === 'd1' ? [{ success: true, results:
    args.at(-1).includes('sqlite_master')
      ? [{ sql: 'write_generation INTEGER NOT NULL, operation_state TEXT' }]
      : args.at(-1).includes('COUNT(*) AS count')
        ? [{ count: 0 }]
        : args.at(-1).includes('FROM flowsafe_remote_step_invocations i')
          ? [{ status: 'settled', settlement_outcome: 'applied',
            operation_state: 'resume-admitted', thread_id: 'thread',
            notification_id: 'notification', receipt_key: 'key',
            receipt_digest: 'a'.repeat(64), receipt_thread_id: 'thread',
            receipt_notification_id: 'notification',
            stored_notification_id: 'notification', source: 'flowsafe-staging',
            kind: 'host-owned-notification' }]
          : [{ notifications: counts, receipts: counts++ }],
  }] : null;
  if (command === 'deploy') {
    writeFileSync(process.env.TEST_DEPLOY_STARTED_FILE, 'started');
    while (!existsSync(process.env.TEST_RELEASE_DEPLOY_FILE))
      originalSpawnSync('sleep', ['0.02']);
    writeFileSync(process.env.TEST_OFF_FILE, 'deployed');
    return { status: 0, stdout: 'deployed' };
  }
  if (response === null) throw new Error('unexpected Wrangler command');
  return { status: 0, stdout: JSON.stringify(response) };
};
syncBuiltinESMExports();
`,
  );

  const requests = [];
  let fence = {
    state: 'migration-locked',
    mutationEpoch: 0,
    requireMutationEpoch: false,
    transitionRevision: 0,
  };
  let startReceived = false;
  const server = createServer(async (request, response) => {
    assert.equal(request.headers.authorization, `Bearer ${secret}`);
    const path = request.url;
    const method = request.method;
    requests.push(`${method} ${path}`);
    if (method === 'POST' && path === '/start') {
      startReceived = true;
      if (outcome === 'parent death') return;
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(
        JSON.stringify({ status: 'suspended', invocationId: 'invocation' }),
      );
      return;
    }
    let value;
    if (path === '/complete') {
      value = { invocationId: 'invocation', status: 'success' };
    } else if (path === '/admin/maintenance-status') {
      value = { alarmAt: 1, nextRemoteStepRecoveryAt: 1 };
    } else if (path === '/admin/execution-fence' && method === 'POST') {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      assert.equal(body.expected, fence.state);
      assert.equal(body.expectedMutationEpoch, fence.mutationEpoch);
      assert.equal(body.expectedRevision, fence.transitionRevision);
      assert.equal(body.advanceMutationEpoch, true);
      fence = {
        state: body.next,
        mutationEpoch: fence.mutationEpoch + 1,
        requireMutationEpoch: true,
        transitionRevision: fence.transitionRevision + 1,
      };
      value = fence;
    } else {
      assert.equal(path, '/admin/execution-fence');
      value = fence;
    }
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify(value));
  });
  let controller;
  let watcherPid;
  try {
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = server.address().port;
    controller = spawn(
      process.execPath,
      [
        fileURLToPath(
          new URL('./notification-staging-live.mjs', import.meta.url),
        ),
        '--run',
      ],
      {
        env: {
          ...process.env,
          NODE_OPTIONS: `--import=${preloadFile}`,
          TMPDIR: directory,
          FLOWSAFE_STAGING_PROBE_PORT: String(port),
          FLOWSAFE_STAGING_ENABLED_VERSION: enabled,
          FLOWSAFE_STAGING_PROBE_SECRET_FILE: secretFile,
          FLOWSAFE_CF_TOKEN_FILE: tokenFile,
          FLOWSAFE_CF_ACCOUNT_ID: accountId,
          TEST_OFF_FILE: offFile,
          TEST_OFF_CONFIRMED_FILE: offConfirmedFile,
          TEST_WATCHDOG_PID_FILE: watcherPidFile,
          TEST_DEPLOY_STARTED_FILE: deployStartedFile,
          TEST_RELEASE_DEPLOY_FILE: releaseDeployFile,
          TEST_CLOCK_JUMP: outcome === 'controller request' ? 'true' : 'false',
        },
        stdio: 'ignore',
      },
    );
    const stateFile = join(
      directory,
      `flowsafe-notification-control-${controller.pid}`,
      'watchdog.json',
    );
    const deadline = Date.now() + 8_000;
    const waitFor = async (condition) => {
      while (!condition()) {
        assert(Date.now() < deadline, 'watchdog process timed out');
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    };
    await waitFor(() => existsSync(`${stateFile}.ready`));
    await waitFor(() => existsSync(watcherPidFile));
    watcherPid = Number(readFileSync(watcherPidFile, 'utf8'));
    await waitFor(() => startReceived);
    assert.equal(existsSync(offFile), false);
    if (outcome === 'parent death') {
      assert.equal(fence.state, 'open');
      controller.kill('SIGKILL');
    }
    await waitFor(() => existsSync(deployStartedFile));
    assert.equal(
      spawnSync('flock', ['--nonblock', lockFile, 'true']).status,
      1,
    );
    writeFileSync(releaseDeployFile, 'release');
    await waitFor(() => existsSync(offConfirmedFile));
    assert.equal(readFileSync(offFile, 'utf8'), 'deployed');
    assert.deepEqual(fence, {
      state: 'migration-locked',
      mutationEpoch: 2,
      requireMutationEpoch: true,
      transitionRevision: 2,
    });
    assert.deepEqual(
      requests.filter((request) => request === 'POST /admin/execution-fence'),
      ['POST /admin/execution-fence', 'POST /admin/execution-fence'],
    );
    if (outcome === 'controller request') {
      await waitFor(() => controller.exitCode !== null);
      assert.equal(controller.exitCode, 0);
    }
    await waitFor(
      () => spawnSync('flock', ['--nonblock', lockFile, 'true']).status === 0,
    );
  } finally {
    controller?.kill('SIGKILL');
    if (Number.isInteger(watcherPid)) {
      try {
        process.kill(watcherPid, 'SIGKILL');
      } catch {
        // The watchdog may have exited after the off deployment.
      }
    }
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    rmSync(directory, { recursive: true, force: true });
  }
}

test('watchdog owns off deployment after parent death', () =>
  exerciseWatchdog('parent death'));

test('watchdog owns off deployment after controller request', () =>
  exerciseWatchdog('controller request'));

test('controller refuses a contended deployment lock before Wrangler preflight', async () => {
  const directory = mkdtempSync(
    join(tmpdir(), 'flowsafe-lock-contention-test-'),
  );
  const accountId = 'e'.repeat(32);
  const lockDirectory = join(
    '/tmp',
    `flowsafe-deploy-locks-${process.getuid()}`,
  );
  const lockFile = join(
    lockDirectory,
    `${accountId}-anchorage-flowsafe-remote-step-staging.lock`,
  );
  mkdirSync(lockDirectory, { recursive: true, mode: 0o700 });
  writeFileSync(lockFile, '', { mode: 0o600 });
  const secretFile = join(directory, 'probe-secret');
  const tokenFile = join(directory, 'cf-token');
  writeFileSync(secretFile, 's'.repeat(32), { mode: 0o600 });
  writeFileSync(tokenFile, 'fake-token', { mode: 0o600 });
  const holder = spawn('flock', ['--exclusive', lockFile, 'sleep', '5'], {
    stdio: 'ignore',
  });
  try {
    const deadline = Date.now() + 3_000;
    while (spawnSync('flock', ['--nonblock', lockFile, 'true']).status !== 1) {
      assert(Date.now() < deadline, 'lock holder did not acquire lock');
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    const result = spawnSync(
      process.execPath,
      [
        fileURLToPath(
          new URL('./notification-staging-live.mjs', import.meta.url),
        ),
        '--run',
      ],
      {
        encoding: 'utf8',
        env: {
          ...process.env,
          FLOWSAFE_STAGING_PROBE_PORT: '12345',
          FLOWSAFE_STAGING_ENABLED_VERSION: enabled,
          FLOWSAFE_STAGING_PROBE_SECRET_FILE: secretFile,
          FLOWSAFE_CF_TOKEN_FILE: tokenFile,
          FLOWSAFE_CF_ACCOUNT_ID: accountId,
        },
      },
    );
    assert.equal(result.status, 1);
    assert.match(result.stderr, /deployment lock is unavailable/);
    assert.doesNotMatch(result.stderr, /Wrangler/);
  } finally {
    holder.kill('SIGKILL');
    rmSync(directory, { recursive: true, force: true });
  }
});
