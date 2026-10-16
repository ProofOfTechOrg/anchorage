import { spawn, spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  EXECUTION_FENCE_DDL,
  EXECUTION_FENCE_ROW_ID,
  EXECUTION_FENCE_TABLE,
} from '#deployment-identity-protocol';
import {
  createWorkerdServerLifecycle,
  parsePort,
} from '../../../scripts/workerd-server-lifecycle.mjs';

const flowsafe = dirname(dirname(fileURLToPath(import.meta.url)));
const wrangler = join(flowsafe, '../../node_modules/.bin/wrangler');
const config = join(flowsafe, 'deploy/notification-staging.wrangler.jsonc');
const port = parsePort(
  process.env.NOTIFICATION_STAGING_NATIVE_PORT ?? 8897,
  'NOTIFICATION_STAGING_NATIVE_PORT',
);
const tag = 'flowsafenotificationstaging';
const database = 'anchorage-flowsafe-remote-step-staging-db';
const secret = () => randomBytes(32).toString('hex');
const deploymentSecret = secret();
const maintenanceSecret = secret();
const probeSecret = secret();
const lifecycle = createWorkerdServerLifecycle({ port });
let stateDir;
let serverOutput = '';

function workerdDiagnostics() {
  return serverOutput
    .replaceAll(deploymentSecret, '[redacted]')
    .replaceAll(maintenanceSecret, '[redacted]')
    .replaceAll(probeSecret, '[redacted]');
}

function expect(condition, message, detail) {
  if (!condition)
    throw new Error(
      `${message}${detail === undefined ? '' : `: ${JSON.stringify(detail)}`}`,
    );
}

function localD1(command) {
  const result = spawnSync(
    wrangler,
    [
      'd1',
      'execute',
      database,
      '--local',
      '--yes',
      '--json',
      '--config',
      config,
      '--persist-to',
      stateDir,
      '--command',
      command,
    ],
    { cwd: flowsafe, encoding: 'utf8', timeout: 30_000 },
  );
  expect(
    result.status === 0,
    'local D1 query failed',
    result.stderr?.slice(-800),
  );
  return JSON.parse(result.stdout);
}

async function post(path, authorization, body) {
  const response = await fetch(`http://127.0.0.1:${port}${path}`, {
    method: 'POST',
    headers: {
      ...(authorization ? { 'x-flowsafe-staging-probe': authorization } : {}),
      ...(body ? { 'content-type': 'application/json' } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
    signal: AbortSignal.timeout(90_000),
  });
  const text = await response.text();
  let value;
  try {
    value = JSON.parse(text);
  } catch {
    value = text.slice(0, 1000);
  }
  return { status: response.status, value };
}

async function run() {
  await lifecycle.preflight();
  stateDir = mkdtempSync(join(tmpdir(), 'flowsafe-notification-native-'));
  localD1(
    'CREATE TABLE IF NOT EXISTS flowsafe_deployment (id INTEGER PRIMARY KEY CHECK (id = 1), tenant_tag TEXT NOT NULL, provisioned_at TEXT NOT NULL); ' +
      `INSERT OR IGNORE INTO flowsafe_deployment (id, tenant_tag, provisioned_at) VALUES (1, '${tag}', datetime('now')); ` +
      `${EXECUTION_FENCE_DDL}; ` +
      `INSERT OR IGNORE INTO ${EXECUTION_FENCE_TABLE} (id, state, proof_key, proof_run_id, updated_at) ` +
      `VALUES ('${EXECUTION_FENCE_ROW_ID}', 'migration-locked', NULL, NULL, ${Date.now()});`,
  );
  await lifecycle.start('notification-staging-native', () => {
    const child = spawn(
      wrangler,
      [
        'dev',
        '--config',
        config,
        '--local',
        '--port',
        String(port),
        '--ip',
        '127.0.0.1',
        '--inspector-port',
        '0',
        '--persist-to',
        stateDir,
        '--var',
        `DEPLOYMENT_TENANT:${tag}`,
        '--var',
        `DEPLOYMENT_IDENTITY_SECRET:${deploymentSecret}`,
        '--var',
        `MAINTENANCE_ADMIN_SECRET:${maintenanceSecret}`,
        '--var',
        `STAGING_PROBE_SECRET:${probeSecret}`,
        '--var',
        'NOTIFICATION_PROOF_ENABLED:true',
        '--var',
        'STAGING_MUTATION_EPOCH:1',
      ],
      {
        cwd: flowsafe,
        detached: true,
        stdio: ['ignore', 'pipe', 'pipe'],
        env: { ...process.env, WRANGLER_SEND_METRICS: 'false', NO_COLOR: '1' },
      },
    );
    const capture = (chunk) => {
      serverOutput = `${serverOutput}${chunk.toString()}`.slice(-30000);
    };
    child.stdout.on('data', capture);
    child.stderr.on('data', capture);
    return { child, generation: 'notification-staging-native' };
  });
  const refused = await post('/__staging/start');
  expect(
    refused.status === 403,
    'private start admitted an unauthenticated call',
    refused,
  );
  const opened = await fetch(`http://127.0.0.1:${port}/admin/execution-fence`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${maintenanceSecret}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      expected: 'migration-locked',
      next: 'open',
      expectedMutationEpoch: 0,
      expectedRevision: 0,
      advanceMutationEpoch: true,
    }),
    signal: AbortSignal.timeout(90_000),
  });
  expect(opened.status === 200, 'native execution fence did not open');
  const fence = await opened.json();
  expect(
    fence.mutationEpoch === 1 && fence.requireMutationEpoch === true,
    'native execution fence did not require the enabled epoch',
    fence,
  );
  const started = await post('/__staging/start', probeSecret);
  expect(started.status === 200, 'guarded start/register failed', {
    response: started,
    workerd: workerdDiagnostics(),
  });
  expect(
    started.value.status === 'suspended',
    'guarded agent did not suspend',
    started,
  );
  expect(
    typeof started.value.invocationId === 'string',
    'journal registration did not return an invocation',
    started,
  );
  const completed = await post('/__staging/complete', probeSecret, {
    invocationId: started.value.invocationId,
  });
  expect(completed.status === 200, 'selected notification completion failed', {
    response: completed,
    workerd: workerdDiagnostics(),
  });
  const rows = localD1(
    `SELECT COUNT(*) AS count FROM mastra_notifications WHERE source = 'flowsafe-staging' AND kind = 'host-owned-notification'`,
  );
  expect(
    rows?.[0]?.results?.[0]?.count === 1,
    'selected notification was not retained exactly once',
    rows,
  );
  console.log(
    JSON.stringify({
      verdict: 'native_notification_staging_passed',
      runId: started.value.runId,
      invocationId: started.value.invocationId,
      completionStatus: completed.value.status,
      notificationCount: 1,
    }),
  );
}

try {
  await run();
} catch (error) {
  console.error(workerdDiagnostics());
  throw error;
} finally {
  await lifecycle.stop();
  if (stateDir) rmSync(stateDir, { recursive: true, force: true });
}
