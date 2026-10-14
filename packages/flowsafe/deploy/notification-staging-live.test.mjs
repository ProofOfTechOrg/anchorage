import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import {
  existsSync,
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
  relockIfOpen,
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

test('watchdog relock uses a fresh epoch and revision', async () => {
  const calls = [];
  let observedRevision = 1;
  const probe = async (method, path, body) => {
    calls.push({ method, path, body });
    if (method === 'GET')
      return {
        state: 'open',
        mutationEpoch: 1,
        transitionRevision: observedRevision,
        requireMutationEpoch: true,
      };
    return {
      state: 'migration-locked',
      mutationEpoch: 2,
      transitionRevision: 2,
      requireMutationEpoch: true,
    };
  };
  assert.equal(
    (await relockIfOpen(probe, { mutationEpoch: 1, transitionRevision: 1 }))
      .state,
    'migration-locked',
  );
  assert.deepEqual(calls[1].body, {
    expected: 'open',
    next: 'migration-locked',
    expectedMutationEpoch: 1,
    expectedRevision: 1,
    advanceMutationEpoch: true,
  });
  calls.length = 0;
  observedRevision = 2;
  await assert.rejects(
    relockIfOpen(probe, { mutationEpoch: 1, transitionRevision: 1 }),
    /metadata differs/,
  );
  assert.equal(calls.length, 1);
});

test('controller waits for watchdog readiness and watchdog recovers after parent death', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'flowsafe-live-watchdog-test-'));
  const secret = 's'.repeat(32);
  const tokenFile = join(directory, 'cf-token');
  const secretFile = join(directory, 'probe-secret');
  const preloadFile = join(directory, 'fake-wrangler.mjs');
  const offFile = join(directory, 'off-deploy');
  const offConfirmedFile = join(directory, 'off-confirmed');
  const watcherPidFile = join(directory, 'watchdog-pid');
  writeFileSync(tokenFile, 'fake-token', { mode: 0o600 });
  writeFileSync(secretFile, secret, { mode: 0o600 });
  writeFileSync(
    preloadFile,
    `import childProcess from 'node:child_process';
import { syncBuiltinESMExports } from 'node:module';
import { existsSync, writeFileSync } from 'node:fs';
const originalSpawn = childProcess.spawn;
childProcess.spawn = (...args) => {
  const child = originalSpawn(...args);
  if (args[1]?.includes('--watchdog'))
    writeFileSync(process.env.TEST_WATCHDOG_PID_FILE, String(child.pid));
  return child;
};
childProcess.spawnSync = (executable, args) => {
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
        : [{ notifications: 0, receipts: 0 }],
  }] : null;
  if (command === 'deploy') {
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
      return;
    }
    let value;
    if (path === '/admin/maintenance-status') {
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
          TEST_OFF_FILE: offFile,
          TEST_OFF_CONFIRMED_FILE: offConfirmedFile,
          TEST_WATCHDOG_PID_FILE: watcherPidFile,
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
    assert(existsSync(`${stateFile}.unlock-attempted`));
    assert.equal(fence.state, 'open');
    assert.equal(existsSync(offFile), false);
    controller.kill('SIGKILL');
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
    assert.equal(existsSync(`${stateFile}.done`), false);
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
});
