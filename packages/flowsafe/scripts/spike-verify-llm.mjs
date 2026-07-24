// Credentialed closeout proof for the real per-thread durable-agent loop.
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const FLOWSAFE = dirname(dirname(fileURLToPath(import.meta.url)));
const WRANGLER = join(FLOWSAFE, 'node_modules/.bin/wrangler');
const CONFIG = join(FLOWSAFE, 'spike/wrangler.jsonc');
const PORT = Number(process.env.SPIKE_LLM_PORT ?? 8801);
const BASE = `http://127.0.0.1:${PORT}`;
const MODEL_ID =
  process.env.SPIKE_LLM_MODEL_ID ??
  (process.env.DEEPSEEK_MODEL
    ? `deepseek/${process.env.DEEPSEEK_MODEL}`
    : undefined);
const API_KEY = process.env.SPIKE_LLM_API_KEY ?? process.env.DEEPSEEK_API_KEY;
const BASE_URL = process.env.SPIKE_LLM_BASE_URL;
const AUTH = {
  operator: { authorization: 'Bearer spike-operator' },
  reviewer: { authorization: 'Bearer spike-reviewer' },
  viewer: { authorization: 'Bearer spike-viewer' },
};

if (!MODEL_ID?.includes('/') || !API_KEY) {
  throw new Error(
    'SPIKE_LLM_MODEL_ID (provider/model) and SPIKE_LLM_API_KEY are required; DEEPSEEK_MODEL plus DEEPSEEK_API_KEY are accepted aliases; SPIKE_LLM_BASE_URL is optional',
  );
}

const stateDir = mkdtempSync(join(tmpdir(), 'flowsafe-llm-spike-'));
const envFile = join(stateDir, '.dev.vars');
writeFileSync(
  envFile,
  [
    `SPIKE_LLM_MODEL_ID=${JSON.stringify(MODEL_ID)}`,
    `SPIKE_LLM_API_KEY=${JSON.stringify(API_KEY)}`,
    ...(BASE_URL ? [`SPIKE_LLM_BASE_URL=${JSON.stringify(BASE_URL)}`] : []),
  ].join('\n'),
  { mode: 0o600 },
);
let server;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const assert = (condition, message, detail) => {
  if (!condition) {
    throw new Error(
      `${message}${detail === undefined ? '' : `: ${JSON.stringify(detail)}`}`,
    );
  }
};

function workerdDiagnostics() {
  return server.output
    .join('')
    .replaceAll(API_KEY, '[REDACTED]')
    .slice(-20_000);
}

async function request(method, path, options = {}) {
  const response = await fetch(`${BASE}${path}`, {
    method,
    headers: {
      ...(options.body === undefined
        ? {}
        : { 'content-type': 'application/json' }),
      ...(options.headers ?? {}),
    },
    ...(options.body === undefined
      ? {}
      : { body: JSON.stringify(options.body) }),
  });
  const text = await response.text();
  let body;
  try {
    body = text ? JSON.parse(text) : undefined;
  } catch {
    body = text;
  }
  return { status: response.status, body };
}

function start() {
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
      stateDir,
      '--env-file',
      envFile,
    ],
    {
      cwd: FLOWSAFE,
      detached: true,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, WRANGLER_SEND_METRICS: 'false', NO_COLOR: '1' },
    },
  );
  const output = [];
  child.stdout.on('data', (chunk) => output.push(chunk.toString()));
  child.stderr.on('data', (chunk) => output.push(chunk.toString()));
  return { child, output };
}

async function portOpen() {
  return new Promise((resolve) => {
    const socket = net.connect({ port: PORT, host: '127.0.0.1' });
    socket.setTimeout(500);
    socket.once('connect', () => {
      socket.destroy();
      resolve(true);
    });
    const closed = () => {
      socket.destroy();
      resolve(false);
    };
    socket.once('error', closed);
    socket.once('timeout', closed);
  });
}

async function waitReady() {
  const deadline = Date.now() + 90_000;
  while (Date.now() < deadline) {
    if (await portOpen()) return;
    if (server.child.exitCode !== null) {
      throw new Error(`wrangler exited early: ${server.output.join('')}`);
    }
    await sleep(250);
  }
  throw new Error(`wrangler did not listen: ${server.output.join('')}`);
}

async function stop() {
  if (!server) return;
  try {
    process.kill(-server.child.pid, 'SIGKILL');
  } catch {}
  const deadline = Date.now() + 15_000;
  while ((await portOpen()) && Date.now() < deadline) await sleep(200);
  server = undefined;
}

async function waitSuspended(ids) {
  const deadline = Date.now() + 180_000;
  let lastStatus;
  while (Date.now() < deadline) {
    const status = await request(
      'GET',
      `/agent/live/status?threadId=${encodeURIComponent(ids.threadId)}&resourceId=${encodeURIComponent(ids.resourceId)}&runId=${encodeURIComponent(ids.runId)}`,
      { headers: AUTH.operator },
    );
    lastStatus = status;
    if (status.body?.status === 'suspended')
      return { ...ids, summary: status.body };
    if (status.body?.status === 'failed')
      throw new Error(`agent failed: ${JSON.stringify(status.body)}`);
    if (status.body?.status === 'success') {
      throw new Error(
        `agent completed without the required approval suspension: ${JSON.stringify(status.body)}`,
      );
    }
    await sleep(500);
  }
  throw new Error(
    `agent did not suspend: ${JSON.stringify({ ...ids, lastStatus, workerdOutput: workerdDiagnostics() })}`,
  );
}

async function startSuspended() {
  const started = await request('POST', '/agent/live/start', {
    headers: AUTH.operator,
    body: { prompt: 'Use the required write tool exactly once.' },
  });
  assert(started.status === 200, 'agent start failed', started);
  return waitSuspended({
    threadId: started.body.threadId,
    resourceId: started.body.resourceId,
    runId: started.body.runId,
  });
}

async function effects() {
  const result = await request('GET', '/agent/live/effects', {
    headers: AUTH.operator,
  });
  assert(result.status === 200, 'effect count failed', result);
  return result.body.count;
}

try {
  console.log(
    '> start workerd and drive three real model tool calls to suspension',
  );
  server = start();
  await waitReady();
  const actual = await startSuspended();
  const raw = await startSuspended();
  const forged = await startSuspended();
  assert((await effects()) === 0, 'write occurred before approval');

  console.log('> kill workerd and restart on persisted D1');
  await stop();
  server = start();
  await waitReady();

  console.log(
    '> due agent schedule reaches the durable thread loop and approval queue',
  );
  const scheduled = await request('POST', '/sched/agent');
  assert(
    scheduled.status === 200 && scheduled.body.result?.fired === 1,
    'agent schedule did not fire',
    scheduled,
  );
  await waitSuspended(scheduled.body);
  const scheduleApprovals = await request('GET', '/api/approvals', {
    headers: AUTH.viewer,
  });
  assert(
    scheduleApprovals.body.some(
      (record) =>
        record.runId === scheduled.body.runId &&
        record.resumeTarget?.threadId === scheduled.body.threadId,
    ),
    'agent schedule did not reach the approval queue',
    scheduleApprovals.body,
  );

  console.log('> raw resume without prepare has no effect');
  const rawResume = await request('POST', '/agent/live/raw-resume', {
    headers: AUTH.operator,
    body: raw,
  });
  await sleep(500);
  assert(
    rawResume.status === 404 &&
      String(rawResume.body?.error ?? '').includes('unknown workflow'),
    'raw resume did not fail closed',
    rawResume,
  );
  assert((await effects()) === 0, 'raw resume executed a write');

  console.log(
    '> prepared forged resume reaches the connector but has no grant',
  );
  const forgedResume = await request('POST', '/agent/live/prepared-resume', {
    headers: AUTH.operator,
    body: forged,
  });
  await sleep(500);
  const forgedResult = JSON.stringify(forgedResume.body);
  assert(
    forgedResume.status === 200 &&
      forgedResult.includes('approval required and not granted'),
    'prepared forged resume did not fail at the connector approval gate',
    { forgedResume, workerdOutput: workerdDiagnostics() },
  );
  assert(
    (await effects()) === 0,
    'unapproved prepared resume executed a write',
  );

  const listed = await request('GET', '/api/approvals', {
    headers: AUTH.viewer,
  });
  const approval = listed.body.find(
    (record) => record.runId === actual.runId && record.status === 'pending',
  );
  assert(
    approval?.resumeTarget?.threadId === actual.threadId,
    'trusted resume target missing',
    approval,
  );
  assert(
    approval.connectors?.length === 1 &&
      approval.connectors[0] === 'spike_recordWrite',
    'approval did not grant the provider-safe connector id',
    approval,
  );

  console.log(
    '> approve through the queue; fresh thread DO prepares then resumes',
  );
  const decided = await request(
    'POST',
    `/api/approvals/${encodeURIComponent(approval.id)}/decide`,
    { headers: AUTH.reviewer, body: { decision: 'approve' } },
  );
  assert(
    decided.status === 200 && decided.body.resume?.ok === true,
    'approval resume failed',
    decided,
  );
  assert(
    decided.body.resume.summary?.status === 'success',
    'agent did not succeed',
    decided.body.resume,
  );
  assert((await effects()) === 1, 'connector side effect was not exactly once');
  console.log(
    '\nLLM durable-agent closeout passed: suspend → kill → prepare → approve → exactly-once write.',
  );
} finally {
  await stop();
  rmSync(stateDir, { recursive: true, force: true });
}
