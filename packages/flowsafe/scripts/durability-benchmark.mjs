import { spawn } from 'node:child_process';
import { createWriteStream, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  createSpikeServerLifecycle,
  parseSpikePort,
} from './spike-server-lifecycle.mjs';

const FLOWSAFE = dirname(dirname(fileURLToPath(import.meta.url)));
const WRANGLER = join(FLOWSAFE, 'node_modules/.bin/wrangler');
const CONFIG = join(FLOWSAFE, 'spike/durability-benchmark.wrangler.jsonc');
const PORT = parseSpikePort(
  process.env.DURABILITY_BENCHMARK_PORT ?? 8801,
  'DURABILITY_BENCHMARK_PORT',
);
const BASE = `http://127.0.0.1:${PORT}`;
const lifecycle = createSpikeServerLifecycle({ port: PORT });
const servers = [];
let currentServer;
let tempDirectory;

function assert(condition, label, detail) {
  if (condition) return;
  const suffix =
    detail === undefined ? '' : `\n  got: ${JSON.stringify(detail)}`;
  throw new Error(`${label}${suffix}`);
}

async function step(label, operation) {
  const startedAt = Date.now();
  process.stdout.write(`> ${label}\n`);
  const result = await operation();
  process.stdout.write(
    `  ok (${((Date.now() - startedAt) / 1000).toFixed(1)}s)\n`,
  );
  return result;
}

function startServer(generation, stateDirectory, logPath) {
  const child = spawn(
    WRANGLER,
    [
      'dev',
      '--config',
      CONFIG,
      '--port',
      String(PORT),
      '--ip',
      '127.0.0.1',
      '--inspector-port',
      '0',
      '--persist-to',
      stateDirectory,
    ],
    {
      cwd: FLOWSAFE,
      detached: true,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, WRANGLER_SEND_METRICS: 'false', NO_COLOR: '1' },
    },
  );
  const chunks = [];
  const file = createWriteStream(logPath, { flags: 'a' });
  const capture = (chunk) => {
    chunks.push(chunk.toString());
    file.write(chunk);
  };
  child.stdout.on('data', capture);
  child.stderr.on('data', capture);
  const server = { generation, child, chunks, file, logPath };
  child.once('error', (error) => {
    server.spawnError = error;
  });
  servers.push(server);
  return server;
}

function executeLocalD1(stateDirectory, command) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      WRANGLER,
      [
        'd1',
        'execute',
        'flowsafe-durability-benchmark',
        '--local',
        '--yes',
        '--config',
        CONFIG,
        '--persist-to',
        stateDirectory,
        '--command',
        command,
      ],
      {
        cwd: FLOWSAFE,
        stdio: ['ignore', 'pipe', 'pipe'],
        env: { ...process.env, WRANGLER_SEND_METRICS: 'false', NO_COLOR: '1' },
      },
    );
    let output = '';
    child.stdout.on('data', (chunk) => {
      output += chunk.toString();
    });
    child.stderr.on('data', (chunk) => {
      output += chunk.toString();
    });
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) return resolve();
      reject(
        new Error(
          `wrangler d1 execute failed (${signal ?? `exit ${code}`}): ${output.slice(-2000)}`,
        ),
      );
    });
  });
}

async function launch(generation, stateDirectory) {
  currentServer = await lifecycle.start(
    generation,
    () =>
      startServer(
        generation,
        stateDirectory,
        join(tempDirectory, `${generation}.log`),
      ),
    90_000,
  );
}

async function kill() {
  await lifecycle.stop();
  currentServer = undefined;
}

async function http(method, path, body) {
  const response = await fetch(`${BASE}${path}`, {
    method,
    headers:
      body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(30_000),
  });
  const text = await response.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error(
      `${method} ${path} returned non-JSON ${response.status}: ${text}`,
    );
  }
  return { status: response.status, body: parsed };
}

const get = (path) => http('GET', path);
const post = (path, body = {}) => http('POST', path, body);

async function waitFor(path, predicate, label, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  let latest;
  while (Date.now() < deadline) {
    latest = await get(path);
    if (predicate(latest)) return latest;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(
    `${label} did not converge\n  got: ${JSON.stringify(latest)}`,
  );
}

function approvalOf(response) {
  const approval = response.body.approvals?.[0];
  assert(
    typeof approval?.id === 'string',
    'suspended run has one approval',
    response,
  );
  return approval;
}

async function effectCount(authority, runId) {
  const response = await get(
    `/effects?authority=${authority}&runId=${encodeURIComponent(runId)}`,
  );
  assert(response.status === 200, 'effect ledger is readable', response);
  return response.body.effects.length;
}

function dumpLog(server) {
  const tail = server.chunks.join('').split('\n').slice(-160);
  process.stderr.write(
    `\n--- ${server.generation} log tail (${server.logPath}) ---\n`,
  );
  process.stderr.write(`${tail.join('\n')}\n`);
}

async function cleanup() {
  await lifecycle.cleanup(() => {
    for (const server of servers) server.file.end();
    if (tempDirectory !== undefined) {
      rmSync(tempDirectory, { recursive: true, force: true });
      tempDirectory = undefined;
    }
  });
  currentServer = undefined;
}

async function main() {
  await step(`preflight: port ${PORT} is free`, () => lifecycle.preflight());
  tempDirectory = mkdtempSync(join(tmpdir(), 'flowsafe-durability-benchmark-'));
  const stateDirectory = join(tempDirectory, 'state');
  await step('seed the real local D1 deployment sentinel', () =>
    executeLocalD1(
      stateDirectory,
      "CREATE TABLE IF NOT EXISTS flowsafe_deployment (id INTEGER PRIMARY KEY CHECK (id = 1), tenant_tag TEXT NOT NULL, provisioned_at TEXT NOT NULL); INSERT OR IGNORE INTO flowsafe_deployment (id, tenant_tag, provisioned_at) VALUES (1, 'benchmark', datetime('now'));",
    ),
  );
  await step('generation 1: launch Wrangler/workerd', () =>
    launch('generation-1', stateDirectory),
  );

  await step(
    'comparison: Cloudflare WorkflowEntrypoint completes the hand-mapped graph',
    async () => {
      const started = await post('/native/deliveries', {
        deliveryId: 'native-delivery-a',
        runId: 'native-run-a',
        topic: 'launch',
      });
      assert(started.status === 201, 'native workflow starts', started);
      const duplicateDelivery = await post('/native/deliveries', {
        deliveryId: 'native-delivery-a',
        runId: 'native-run-forged',
        topic: 'launch',
      });
      assert(
        duplicateDelivery.status === 200 &&
          duplicateDelivery.body.deduplicated === true &&
          duplicateDelivery.body.runId === 'native-run-a',
        'native provider delivery deduplicates onto the original run identity',
        duplicateDelivery,
      );
      const approval = await post('/native/runs/native-run-a/approve');
      assert(
        approval.status === 200,
        'native approval event accepted',
        approval,
      );
      const duplicateEvent = await post('/native/runs/native-run-a/event');
      assert(
        duplicateEvent.status === 200,
        'duplicate native event accepted without a second effect',
        duplicateEvent,
      );
      const completed = await waitFor(
        '/native/runs/native-run-a',
        (response) => response.body.summary?.status === 'complete',
        'native workflow completion',
      );
      assert(
        completed.body.summary.output?.grantReconstructed === true &&
          completed.body.summary.output?.effectCount === 1,
        'native workflow reconstructs its D1 approval and records one effect',
        completed,
      );
      const duplicateApproval = await post('/native/runs/native-run-a/approve');
      assert(
        duplicateApproval.status === 409,
        'duplicate native approval conflicts',
        duplicateApproval,
      );
      assert(
        (await effectCount('native', 'native-run-a')) === 1,
        'native comparison effect is at most once',
      );
      const restartCandidate = await post('/native/deliveries', {
        deliveryId: 'native-delivery-restart',
        runId: 'native-run-restart',
        topic: 'restart',
      });
      assert(
        restartCandidate.status === 201,
        'native comparison has a pending run for process-loss recovery',
        restartCandidate,
      );
    },
  );

  const first = await step(
    'matrix: FlowSafe suspends with exact approval provenance',
    async () => {
      const started = await post('/flowsafe/deliveries', {
        deliveryId: 'flowsafe-delivery-a',
        runId: 'flowsafe-run-a',
        topic: 'launch',
      });
      assert(
        started.status === 201 &&
          started.body.summary?.status === 'suspended' &&
          started.body.summary?.suspendedAt?.approval > 0,
        'FlowSafe persists an exact approval suspension',
        started,
      );
      const approval = started.body.approvals?.[0];
      assert(
        approval?.grantScope === 'suspension' &&
          approval.suspendedAt === started.body.summary.suspendedAt.approval,
        'approval grant is bound to the captured suspension',
        approval,
      );
      const duplicate = await post('/flowsafe/deliveries', {
        deliveryId: 'flowsafe-delivery-a',
        runId: 'flowsafe-run-forged',
        topic: 'launch',
      });
      assert(
        duplicate.status === 200 &&
          duplicate.body.deduplicated === true &&
          duplicate.body.runId === 'flowsafe-run-a',
        'duplicate provider delivery retains the first run identity',
        duplicate,
      );
      return { approvalId: approval.id };
    },
  );

  await step(
    'matrix: terminate before approval and restart on persisted state',
    async () => {
      await kill();
      await launch('generation-2', stateDirectory);
      const recovered = await get('/flowsafe/runs/flowsafe-run-a');
      assert(
        recovered.status === 200 &&
          recovered.body.summary?.status === 'suspended' &&
          approvalOf(recovered).id === first.approvalId,
        'suspension and approval survive process loss',
        recovered,
      );
      const nativeRecovered = await get('/native/runs/native-run-restart');
      assert(
        nativeRecovered.status === 200 &&
          ['running', 'waiting'].includes(
            nativeRecovered.body.summary?.status,
          ) &&
          nativeRecovered.body.approval?.status === 'pending',
        'native wait and approval row survive the same process loss',
        nativeRecovered,
      );
      const nativeApproval = await post(
        '/native/runs/native-run-restart/approve',
      );
      assert(
        nativeApproval.status === 200,
        'recovered native run accepts approval',
        nativeApproval,
      );
      const nativeCompleted = await waitFor(
        '/native/runs/native-run-restart',
        (response) => response.body.summary?.status === 'complete',
        'recovered native workflow completion',
      );
      assert(
        nativeCompleted.body.summary.output?.effectCount === 1,
        'recovered native workflow records one effect',
        nativeCompleted,
      );
    },
  );

  await step(
    'matrix: cross-run Durable Object identifiers fail closed',
    async () => {
      const response = await post('/flowsafe/probes/cross-run', {
        objectRunId: 'flowsafe-run-a',
        requestRunId: 'flowsafe-run-cross',
      });
      assert(
        response.status === 200 &&
          response.body.status === 500 &&
          String(response.body.detail).includes('DO identity mismatch'),
        'one run DO refuses a request naming another run',
        response,
      );
    },
  );

  await step(
    'matrix: approve, reconstruct the exact grant, and write one effect',
    async () => {
      const approved = await post('/flowsafe/runs/flowsafe-run-a/approve', {
        approvalId: first.approvalId,
      });
      assert(
        approved.status === 200 &&
          approved.body.resume?.ok === true &&
          approved.body.resume?.summary?.status === 'success' &&
          approved.body.resume?.summary?.result?.grantReconstructed === true &&
          approved.body.resume?.summary?.result?.effectCount === 1,
        'approved FlowSafe run reconstructs its stored grant and succeeds',
        approved,
      );
      const duplicateApproval = await post(
        '/flowsafe/runs/flowsafe-run-a/approve',
        {
          approvalId: first.approvalId,
        },
      );
      assert(
        duplicateApproval.status === 409,
        'duplicate approval loses the CAS',
        duplicateApproval,
      );
      const duplicateResume = await post(
        '/flowsafe/runs/flowsafe-run-a/resume',
      );
      assert(
        duplicateResume.status === 409,
        'duplicate raw resume is refused',
        duplicateResume,
      );
      assert(
        (await effectCount('flowsafe', 'flowsafe-run-a')) === 1,
        'duplicate delivery, decision, and resume cannot repeat the effect',
      );
    },
  );

  await step(
    'matrix: terminate after approval and retain the terminal answer',
    async () => {
      await kill();
      await launch('generation-3', stateDirectory);
      const recovered = await get('/flowsafe/runs/flowsafe-run-a');
      assert(
        recovered.status === 200 &&
          recovered.body.summary?.status === 'success' &&
          recovered.body.summary?.result?.effectCount === 1 &&
          (await effectCount('flowsafe', 'flowsafe-run-a')) === 1,
        'terminal snapshot and effect survive process loss',
        recovered,
      );
    },
  );

  await step(
    'matrix: concurrent reviewers resolve through one D1 CAS',
    async () => {
      const started = await post('/flowsafe/deliveries', {
        deliveryId: 'flowsafe-delivery-contention',
        runId: 'flowsafe-run-contention',
        topic: 'contention',
      });
      assert(started.status === 201, 'contention run starts', started);
      const approvalId = started.body.approvals?.[0]?.id;
      const attempts = await Promise.all([
        post('/flowsafe/runs/flowsafe-run-contention/approve', { approvalId }),
        post('/flowsafe/runs/flowsafe-run-contention/approve', { approvalId }),
      ]);
      assert(
        attempts
          .map((attempt) => attempt.status)
          .sort()
          .join(',') === '200,409',
        'exactly one concurrent decision wins',
        attempts,
      );
      assert(
        (await effectCount('flowsafe', 'flowsafe-run-contention')) === 1,
        'contention winner produces one effect',
      );
    },
  );

  await step(
    'matrix: an approval cannot be replayed against a different run',
    async () => {
      const started = await post('/flowsafe/deliveries', {
        deliveryId: 'flowsafe-delivery-cross-approval',
        runId: 'flowsafe-run-cross-approval',
        topic: 'cross-approval',
      });
      assert(started.status === 201, 'cross-approval target starts', started);
      const replay = await post(
        '/flowsafe/runs/flowsafe-run-cross-approval/approve',
        {
          approvalId: first.approvalId,
        },
      );
      assert(
        replay.status === 409,
        'foreign approval/run pair is refused',
        replay,
      );
      const status = await get('/flowsafe/runs/flowsafe-run-cross-approval');
      assert(
        status.body.summary?.status === 'suspended' &&
          (await effectCount('flowsafe', 'flowsafe-run-cross-approval')) === 0,
        'foreign approval leaves the target suspended and effect-free',
        status,
      );
    },
  );

  process.stdout.write(
    '\nPASS: FlowSafe is the selected durability authority. The real local ' +
      'Wrangler/workerd+D1 matrix passed process loss before/after approval, ' +
      'duplicate provider delivery, duplicate resume/decision, cross-run IDs, ' +
      'D1 decision contention, exact grant reconstruction, and effect-at-most-once.\n',
  );
}

main()
  .catch((error) => {
    process.exitCode = 1;
    process.stderr.write(
      `\nFAIL: ${error instanceof Error ? error.stack : String(error)}\n`,
    );
    if (currentServer) dumpLog(currentServer);
  })
  .finally(async () => {
    try {
      await cleanup();
    } catch (error) {
      process.exitCode = 1;
      process.stderr.write(
        `cleanup failed: ${error instanceof Error ? error.stack : String(error)}\n`,
      );
    }
  });
