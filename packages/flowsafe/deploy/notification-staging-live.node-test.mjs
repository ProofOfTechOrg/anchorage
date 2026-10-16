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
import { dirname, join } from 'node:path';
import { after, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import {
  createWranglerControl,
  sealFence,
} from './notification-staging-live.mjs';

const enabled = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa';
const disabled = 'bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb';
const bundleDirectory = mkdtempSync(
  join(tmpdir(), 'flowsafe-test-off-bundle-'),
);
const bundlePath = join(bundleDirectory, 'notification-staging.worker.js');
writeFileSync(bundlePath, 'export default {};', { mode: 0o600 });
after(() => rmSync(bundleDirectory, { recursive: true, force: true }));

function freshConfig() {
  const source = JSON.parse(
    readFileSync(
      new URL('./notification-staging.wrangler.jsonc', import.meta.url),
      'utf8',
    ),
  );
  source.name = 'anchorage-flowsafe-remote-step-staging-fresh';
  source.main = fileURLToPath(
    new URL('./notification-staging.worker.ts', import.meta.url),
  );
  source.alias = Object.fromEntries(
    Object.entries(source.alias).map(([key, value]) => [
      key,
      fileURLToPath(new URL(value, import.meta.url)),
    ]),
  );
  source.d1_databases[0].database_name = 'anchorage-flowsafe-fresh-db';
  source.d1_databases[0].database_id = 'cccccccc-cccc-4ccc-cccc-cccccccccccc';
  source.vars.DEPLOYMENT_TENANT = 'flowsafenotificationfresh';
  return source;
}

function versionBindings(
  source,
  proofEnabled,
  mutationEpoch = proofEnabled ? 1 : 0,
) {
  return [
    {
      name: 'NOTIFICATION_PROOF_ENABLED',
      type: 'plain_text',
      text: String(proofEnabled),
    },
    ...(mutationEpoch === null
      ? []
      : [
          {
            name: 'STAGING_MUTATION_EPOCH',
            type: 'plain_text',
            text: String(mutationEpoch),
          },
        ]),
    {
      name: 'DEPLOYMENT_TENANT',
      type: 'plain_text',
      text: source.vars.DEPLOYMENT_TENANT,
    },
    {
      name: 'DB',
      type: 'd1',
      database_id: source.d1_databases[0].database_id,
    },
  ];
}

function fakeCli({ enabledFlag = true, priorVersion, mutationEpoch } = {}) {
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
          ...(priorVersion
            ? [
                {
                  created_on: '2026-10-14T23:59:00.000Z',
                  versions: [{ version_id: priorVersion, percentage: 100 }],
                },
              ]
            : []),
        ]),
      };
    }
    if (args[0] === 'versions') {
      const source = JSON.parse(
        readFileSync(
          new URL('./notification-staging.wrangler.jsonc', import.meta.url),
          'utf8',
        ),
      );
      return {
        status: 0,
        stdout: JSON.stringify({
          resources: {
            bindings: versionBindings(
              source,
              args[2] === enabled && enabledFlag,
              mutationEpoch,
            ),
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
    fixture.cli.deployOffIfCurrent(
      enabled,
      '/tmp/staging-off-test',
      bundlePath,
    ),
    disabled,
  );
  assert.equal(fixture.cli.proofEnabled(disabled), false);
  assert.equal(fixture.calls.filter((args) => args[0] === 'deploy').length, 1);
});

test('changed version or disabled flag refuses before deploy', () => {
  const changed = fakeCli();
  changed.change();
  assert.throws(
    () =>
      changed.cli.deployOffIfCurrent(
        enabled,
        '/tmp/staging-off-test',
        bundlePath,
      ),
    /changed/,
  );
  assert.equal(changed.calls.filter((args) => args[0] === 'deploy').length, 0);
  const disabledFlag = fakeCli({ enabledFlag: false });
  assert.throws(
    () =>
      disabledFlag.cli.deployOffIfCurrent(
        enabled,
        '/tmp/staging-off-test',
        bundlePath,
      ),
    /not enabled/,
  );
  assert.equal(
    disabledFlag.calls.filter((args) => args[0] === 'deploy').length,
    0,
  );
});

test('retry evidence requires the immediately prior full off deployment', () => {
  const prior = fakeCli({ priorVersion: disabled });
  assert.equal(prior.cli.priorOffVersion(enabled), disabled);
  const absent = fakeCli();
  assert.throws(() => absent.cli.priorOffVersion(enabled), /history differs/);
  const enabledPrior = fakeCli({ priorVersion: enabled });
  assert.throws(() => enabledPrior.cli.priorOffVersion(enabled), /not off/);
});

test('enabled version must pin the epoch the controller will open', () => {
  for (const epoch of [null, '01', '0', '2']) {
    const fixture = fakeCli({ mutationEpoch: epoch });
    assert.throws(
      () => fixture.cli.proofEnabled(enabled, 1),
      /epoch binding differs/,
    );
  }
  assert.equal(
    fakeCli({ mutationEpoch: 1 }).cli.proofEnabled(enabled, 1),
    true,
  );
  assert.equal(
    fakeCli({ mutationEpoch: 2 }).cli.proofEnabled(enabled, 2),
    true,
  );
});

test('enabled version requires exact D1 and tenant bindings', () => {
  const directory = mkdtempSync(join(tmpdir(), 'flowsafe-d1-mismatch-test-'));
  try {
    const source = freshConfig();
    const path = join(directory, 'private.wrangler.jsonc');
    writeFileSync(path, JSON.stringify(source), { mode: 0o600 });
    let boundDatabaseId = 'dddddddd-dddd-4ddd-dddd-dddddddddddd';
    let boundTenant = source.vars.DEPLOYMENT_TENANT;
    const cli = createWranglerControl({
      configPath: path,
      token: 'fake-token',
      command: (_executable, args) => {
        assert.equal(args[0], 'versions');
        return {
          status: 0,
          stdout: JSON.stringify({
            resources: {
              bindings: [
                {
                  name: 'NOTIFICATION_PROOF_ENABLED',
                  type: 'plain_text',
                  text: 'true',
                },
                {
                  name: 'DEPLOYMENT_TENANT',
                  type: 'plain_text',
                  text: boundTenant,
                },
                {
                  name: 'DB',
                  type: 'd1',
                  database_id: boundDatabaseId,
                },
              ],
            },
          }),
        };
      },
    });
    assert.throws(
      () => cli.proofEnabled(enabled),
      /enabled D1 binding differs/,
    );
    boundDatabaseId = source.d1_databases[0].database_id;
    boundTenant = 'wrongtenant';
    assert.throws(
      () => cli.proofEnabled(enabled),
      /enabled deployment tenant differs/,
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test('private staging config scopes version, D1, and off deployment to one fresh resource', () => {
  const directory = mkdtempSync(join(tmpdir(), 'flowsafe-fresh-config-test-'));
  try {
    const source = freshConfig();
    const path = join(directory, 'private.wrangler.jsonc');
    writeFileSync(path, JSON.stringify(source), { mode: 0o600 });
    const calls = [];
    let currentVersion = enabled;
    const command = (_executable, args) => {
      calls.push(args);
      if (args[0] === 'deployments')
        return {
          status: 0,
          stdout: JSON.stringify([
            {
              created_on: '2026-10-15T00:00:00.000Z',
              versions: [{ version_id: currentVersion, percentage: 100 }],
            },
          ]),
        };
      if (args[0] === 'versions')
        return {
          status: 0,
          stdout: JSON.stringify({
            resources: {
              bindings: versionBindings(source, args[2] === enabled),
            },
          }),
        };
      if (args[0] === 'd1')
        return {
          status: 0,
          stdout: JSON.stringify([{ success: true, results: [] }]),
        };
      if (args[0] === 'deploy') {
        currentVersion = disabled;
        return { status: 0, stdout: 'deployed' };
      }
      throw new Error('unexpected CLI');
    };
    const cli = createWranglerControl({
      command,
      token: 'fake-token',
      configPath: path,
    });
    assert.equal(cli.currentVersion(), enabled);
    assert.deepEqual(cli.query('SELECT 1'), []);
    assert.equal(
      cli.deployOffIfCurrent(enabled, join(directory, 'off'), bundlePath),
      disabled,
    );
    assert.equal(calls.filter((args) => args[0] === 'deploy').length, 1);
    for (const args of calls) {
      assert.equal(args[args.indexOf('--config') + 1], path);
      if (args[0] === 'd1')
        assert.equal(args[2], source.d1_databases[0].database_name);
    }
    source.routes = [{ pattern: 'example.com/*', zone_name: 'example.com' }];
    writeFileSync(path, JSON.stringify(source), { mode: 0o600 });
    assert.throws(
      () => cli.query('SELECT 1'),
      /private staging config differs/,
    );
    assert.throws(
      () =>
        createWranglerControl({
          command,
          token: 'fake-token',
          configPath: path,
        }),
      /private staging config differs/,
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
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

test('watchdog seals the explicit retry baseline against a delayed controller open', async () => {
  let fence = {
    state: 'migration-locked',
    mutationEpoch: 1,
    transitionRevision: 1,
    requireMutationEpoch: true,
  };
  const probe = async (method, _path, body) => {
    if (method === 'GET') return fence;
    assert.deepEqual(body, {
      expected: 'migration-locked',
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
  assert.deepEqual(await sealFence(probe, 1), fence);
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
  const effectStartedFile = join(directory, 'effect-started');
  const fenceFile = join(directory, 'fence.json');
  const deadProbe =
    outcome === 'dead probe after relock' ||
    outcome === 'dead probe while open';
  const accountId =
    outcome === 'parent death'
      ? 'c'.repeat(32)
      : outcome === 'sealed retry'
        ? 'e'.repeat(32)
        : outcome === 'fresh config'
          ? 'f'.repeat(32)
          : outcome === 'mismatched probe'
            ? '1'.repeat(32)
            : 'd'.repeat(32);
  const workerName =
    outcome === 'fresh config'
      ? 'anchorage-flowsafe-remote-step-staging-fresh'
      : 'anchorage-flowsafe-remote-step-staging';
  const lockFile = join(
    '/tmp',
    `flowsafe-deploy-locks-${process.getuid()}`,
    `${accountId}-${workerName}.lock`,
  );
  writeFileSync(tokenFile, 'fake-token', { mode: 0o600 });
  writeFileSync(secretFile, secret, { mode: 0o600 });
  const privateConfigPath = join(directory, 'fresh.wrangler.jsonc');
  const privateProbeConfigPath = join(directory, 'fresh-probe.wrangler.jsonc');
  if (outcome === 'fresh config') {
    writeFileSync(privateConfigPath, JSON.stringify(freshConfig()), {
      mode: 0o600,
    });
    const probeConfig = JSON.parse(
      readFileSync(
        new URL('./notification-staging-probe.wrangler.jsonc', import.meta.url),
        'utf8',
      ),
    );
    probeConfig.main = fileURLToPath(
      new URL('./notification-staging-probe.worker.ts', import.meta.url),
    );
    probeConfig.services[0].service = workerName;
    writeFileSync(privateProbeConfigPath, JSON.stringify(probeConfig), {
      mode: 0o600,
    });
  }
  writeFileSync(
    preloadFile,
    `import childProcess from 'node:child_process';
import { syncBuiltinESMExports } from 'node:module';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
const originalSpawn = childProcess.spawn;
const originalSpawnSync = childProcess.spawnSync;
const originalNow = Date.now;
Date.now = () => originalNow() +
  (process.env.TEST_CLOCK_JUMP === 'true' &&
   !process.argv.includes('--watchdog') &&
   existsSync(process.env.TEST_DEPLOY_STARTED_FILE) ? 300_000 : 0);
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
  if (process.env.TEST_FRESH_CONFIG === 'true') {
    const configPath = args[args.indexOf('--config') + 1];
    if (!configPath.endsWith('/off.wrangler.jsonc'))
      throw new Error('fresh config snapshot was not used');
    if (args[0] === 'd1' && args[2] !== 'anchorage-flowsafe-fresh-db')
      throw new Error('fresh D1 was not selected');
  }
  const off = existsSync(process.env.TEST_OFF_FILE);
  const command = args[0];
  if (off && command === 'deployments')
    writeFileSync(process.env.TEST_OFF_CONFIRMED_FILE, 'checked');
  const sql = args.at(-1);
  const effectStarted = existsSync(process.env.TEST_EFFECT_STARTED_FILE);
  if (command === 'd1' && !effectStarted &&
      (sql.includes('FROM flowsafe_notification_operation_receipts') ||
       sql.includes('FROM mastra_notifications')))
    return { status: 1, stderr: 'notification table is absent' };
  const response = command === 'deployments' ? [{
    created_on: '2026-10-15T00:00:00.000Z',
    versions: [{ version_id: off ? '${disabled}' : '${enabled}', percentage: 100 }],
  }, ...(process.env.TEST_RETRY === 'true' ? [{
    created_on: '2026-10-14T23:59:00.000Z',
    versions: [{ version_id: '${disabled}', percentage: 100 }],
  }] : [])] : command === 'versions' ? {
    resources: { bindings: [{
      name: 'NOTIFICATION_PROOF_ENABLED', type: 'plain_text',
      text: off || args[2] === '${disabled}' ? 'false' : 'true',
    }, {
      name: 'STAGING_MUTATION_EPOCH', type: 'plain_text',
      text: off || args[2] === '${disabled}' ? '0' : process.env.TEST_RETRY === 'true' ? '2' : '1',
    }, {
      name: 'DEPLOYMENT_TENANT', type: 'plain_text',
      text: process.env.TEST_FRESH_CONFIG === 'true'
        ? 'flowsafenotificationfresh' : 'flowsafenotificationstaging',
    }, {
      name: 'DB', type: 'd1',
      database_id: process.env.TEST_FRESH_CONFIG === 'true'
        ? 'cccccccc-cccc-4ccc-cccc-cccccccccccc'
        : 'b863a219-2096-4e13-92fe-4db592b67944',
    }] },
  } : command === 'd1' ? [{ success: true, results:
    sql.includes('FROM flowsafe_execution_fence')
      ? [JSON.parse(readFileSync(process.env.TEST_FENCE_FILE, 'utf8'))]
      : sql.includes("name IN ('mastra_notifications', 'flowsafe_notification_operation_receipts')")
      ? effectStarted ? [{ name: 'mastra_notifications' },
        { name: 'flowsafe_notification_operation_receipts' }] : []
      : sql.includes('sqlite_master')
        ? [{ sql: 'write_generation INTEGER NOT NULL, operation_state TEXT' }]
      : sql.includes('COUNT(*) AS count FROM flowsafe_remote_step_invocations')
        ? [{ count: 0 }]
      : sql.includes('COUNT(*) AS count FROM mastra_notifications') ||
        sql.includes('COUNT(*) AS count FROM flowsafe_notification_operation_receipts')
        ? [{ count: effectStarted ? 1 : 0 }]
        : sql.includes('FROM flowsafe_remote_step_invocations i')
          ? [{ status: 'settled', settlement_outcome: 'applied',
            operation_state: 'resume-admitted', thread_id: 'thread',
            notification_id: 'notification', receipt_key: 'key',
            receipt_digest: 'a'.repeat(64), receipt_thread_id: 'thread',
            receipt_notification_id: 'notification',
            stored_notification_id: 'notification', source: 'flowsafe-staging',
            kind: 'host-owned-notification' }]
          : [],
  }] : null;
  if (command === 'deploy') {
    if (args.includes('--dry-run')) {
      const outdir = args[args.indexOf('--outdir') + 1];
      mkdirSync(outdir, { recursive: true, mode: 0o700 });
      writeFileSync(outdir + '/notification-staging.worker.js', 'export default {};');
      return { status: 0, stdout: 'bundled' };
    }
    if (!args[1]?.endsWith('/pinned-off-bundle/notification-staging.worker.js') ||
        !existsSync(args[1]))
      throw new Error('pinned off bundle was not used');
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
    mutationEpoch: outcome === 'sealed retry' ? 1 : 0,
    requireMutationEpoch: outcome === 'sealed retry',
    transitionRevision: outcome === 'sealed retry' ? 1 : 0,
  };
  let startReceived = false;
  let probeUnavailable = false;
  const recordFence = () =>
    writeFileSync(
      fenceFile,
      JSON.stringify({
        id: 'deployment',
        state: fence.state,
        proof_key: null,
        proof_run_id: null,
        updated_at: 1,
        last_transition_request:
          fence.mutationEpoch === 0 ? null : 'test-transition',
        transition_revision: fence.transitionRevision,
        mutation_epoch: fence.mutationEpoch,
        require_mutation_epoch: Number(fence.requireMutationEpoch),
        proof_table_prefix: null,
        proof_workflow_id: null,
        proof_start_token: null,
      }),
    );
  recordFence();
  const server = createServer(async (request, response) => {
    assert.equal(request.headers.authorization, `Bearer ${secret}`);
    const path = request.url;
    const method = request.method;
    requests.push(`${method} ${path}`);
    if (probeUnavailable) {
      response.destroy();
      return;
    }
    if (method === 'POST' && path === '/start') {
      startReceived = true;
      writeFileSync(effectStartedFile, 'started');
      if (outcome === 'dead probe while open') {
        probeUnavailable = true;
        response.destroy();
        return;
      }
      if (outcome === 'parent death' || outcome === 'fresh config') return;
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(
        JSON.stringify({ status: 'suspended', invocationId: 'invocation' }),
      );
      return;
    }
    let value;
    if (path === '/complete') {
      value = { invocationId: 'invocation', status: 'success' };
    } else if (path === '/identity') {
      value = {
        versionId: outcome === 'mismatched probe' ? disabled : enabled,
        deploymentTenant:
          outcome === 'fresh config'
            ? 'flowsafenotificationfresh'
            : 'flowsafenotificationstaging',
      };
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
      recordFence();
      if (
        outcome === 'dead probe after relock' &&
        fence.state === 'migration-locked'
      )
        probeUnavailable = true;
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
          ...(outcome === 'sealed retry'
            ? { FLOWSAFE_STAGING_RETRY_OFF_VERSION: disabled }
            : {}),
          FLOWSAFE_STAGING_PROBE_SECRET_FILE: secretFile,
          FLOWSAFE_CF_TOKEN_FILE: tokenFile,
          FLOWSAFE_CF_ACCOUNT_ID: accountId,
          ...(outcome === 'fresh config'
            ? {
                FLOWSAFE_STAGING_CONFIG_FILE: privateConfigPath,
                FLOWSAFE_STAGING_PROBE_CONFIG_FILE: privateProbeConfigPath,
              }
            : {}),
          TEST_OFF_FILE: offFile,
          TEST_OFF_CONFIRMED_FILE: offConfirmedFile,
          TEST_WATCHDOG_PID_FILE: watcherPidFile,
          TEST_DEPLOY_STARTED_FILE: deployStartedFile,
          TEST_RELEASE_DEPLOY_FILE: releaseDeployFile,
          TEST_EFFECT_STARTED_FILE: effectStartedFile,
          TEST_FENCE_FILE: fenceFile,
          TEST_CLOCK_JUMP: outcome === 'controller request' ? 'true' : 'false',
          TEST_RETRY: outcome === 'sealed retry' ? 'true' : 'false',
          TEST_FRESH_CONFIG: outcome === 'fresh config' ? 'true' : 'false',
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
    if (outcome === 'fresh config') {
      const state = JSON.parse(readFileSync(stateFile, 'utf8'));
      assert.equal(state.workerName, workerName);
      assert.equal(
        state.configPath,
        join(dirname(stateFile), 'off.wrangler.jsonc'),
      );
      assert.equal(
        readFileSync(state.configPath, 'utf8'),
        readFileSync(privateConfigPath, 'utf8'),
      );
    }
    if (outcome === 'sealed retry')
      assert.equal(
        JSON.parse(readFileSync(stateFile, 'utf8')).baselineEpoch,
        1,
      );
    await waitFor(() => existsSync(watcherPidFile));
    watcherPid = Number(readFileSync(watcherPidFile, 'utf8'));
    if (outcome !== 'mismatched probe') await waitFor(() => startReceived);
    assert.equal(existsSync(offFile), false);
    if (outcome === 'parent death' || outcome === 'fresh config') {
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
      state: outcome === 'dead probe while open' ? 'open' : 'migration-locked',
      mutationEpoch:
        outcome === 'sealed retry'
          ? 3
          : outcome === 'mismatched probe' ||
              outcome === 'dead probe while open'
            ? 1
            : 2,
      requireMutationEpoch: true,
      transitionRevision:
        outcome === 'sealed retry'
          ? 3
          : outcome === 'mismatched probe' ||
              outcome === 'dead probe while open'
            ? 1
            : 2,
    });
    assert.deepEqual(
      requests.filter((request) => request === 'POST /admin/execution-fence'),
      outcome === 'mismatched probe' || outcome === 'dead probe while open'
        ? ['POST /admin/execution-fence']
        : ['POST /admin/execution-fence', 'POST /admin/execution-fence'],
    );
    if (outcome === 'mismatched probe') assert.equal(startReceived, false);
    if (outcome === 'controller request') {
      await waitFor(() => controller.exitCode !== null);
      assert.equal(controller.exitCode, 0);
    }
    await waitFor(
      () => spawnSync('flock', ['--nonblock', lockFile, 'true']).status === 0,
    );
    if (deadProbe) {
      await waitFor(() => controller.exitCode !== null);
      assert.equal(
        controller.exitCode,
        outcome === 'dead probe after relock' ? 0 : 1,
      );
      assert.equal(
        existsSync(`${stateFile}.off-confirmed`),
        outcome === 'dead probe after relock',
      );
    }
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

test('fresh Worker watchdog uses the private off config snapshot after parent death', () =>
  exerciseWatchdog('fresh config'));

test('mismatched authenticated probe subject prevents fence unlock and effect', () =>
  exerciseWatchdog('mismatched probe'));

test('watchdog owns off deployment after controller request', () =>
  exerciseWatchdog('controller request'));

test('sealed retry persists watchdog baseline before unlocking and relocks at three', () =>
  exerciseWatchdog('sealed retry'));

test('watchdog deploys off through probe failure after the controller relocks', () =>
  exerciseWatchdog('dead probe after relock'));

test('watchdog deploys off but refuses confirmation when the dead probe leaves an open fence', () =>
  exerciseWatchdog('dead probe while open'));

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
