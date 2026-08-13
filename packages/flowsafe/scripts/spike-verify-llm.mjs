// Credentialed closeout proof for the real per-thread durable-agent loop.
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  createWorkerdServerLifecycle,
  parsePort,
} from '../../../scripts/workerd-server-lifecycle.mjs';
import { parseLlmSpikeConfig } from './spike-llm-config.mjs';

const FLOWSAFE = dirname(dirname(fileURLToPath(import.meta.url)));
const WRANGLER = join(FLOWSAFE, 'node_modules/.bin/wrangler');
const CONFIG = join(FLOWSAFE, 'spike/wrangler.jsonc');
// Distinct from every other harness default (spike:verify 8799,
// durability-benchmark 8801, conformance:verify 8821) so two can run at once.
const PORT = parsePort(process.env.SPIKE_LLM_PORT ?? 8811, 'SPIKE_LLM_PORT');
const BASE = `http://127.0.0.1:${PORT}`;
const {
  modelId: MODEL_ID,
  apiKey: API_KEY,
  baseUrl: BASE_URL,
} = parseLlmSpikeConfig(process.env);
const AUTH = {
  operator: { authorization: 'Bearer spike-operator' },
  reviewer: { authorization: 'Bearer spike-reviewer' },
  viewer: { authorization: 'Bearer spike-viewer' },
};
const AGENT_ID = 'spike-guarded-agent';

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
const serverLifecycle = createWorkerdServerLifecycle({ port: PORT });

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
  return serverLifecycle.requestJson(
    (recoverySignal) =>
      fetch(`${BASE}${path}`, {
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
        ...(recoverySignal === undefined ? {} : { signal: recoverySignal }),
      }),
    {
      requestLabel: `${method} ${path}`,
      replaySafe: method === 'GET',
    },
  );
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

async function launch() {
  server = await serverLifecycle.start('llm-spike', start, 90_000);
}

async function stop() {
  if (!server) return;
  await serverLifecycle.stop();
  server = undefined;
}

async function waitSuspended(ids) {
  const deadline = Date.now() + 180_000;
  let lastStatus;
  while (Date.now() < deadline) {
    const status = await request(
      'GET',
      `/agents/${AGENT_ID}/runs/${encodeURIComponent(ids.threadId)}/${encodeURIComponent(ids.runId)}`,
      { headers: AUTH.operator },
    );
    lastStatus = status;
    assert(status.status === 200, 'agent status failed', status);
    if (status.body?.summary?.status === 'suspended') return status.body;
    if (status.body?.summary?.status === 'failed')
      throw new Error(`agent failed: ${JSON.stringify(status.body)}`);
    if (status.body?.summary?.status === 'success') {
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
  const started = await request('POST', `/agents/${AGENT_ID}/runs`, {
    headers: AUTH.operator,
    body: { prompt: 'Use the required write tool exactly once.' },
  });
  assert(started.status === 200, 'agent start failed', started);
  return started.body?.summary?.status === 'suspended'
    ? started.body
    : waitSuspended(started.body);
}

async function effects(runId) {
  const result = await request(
    'GET',
    `/agent/live/effects?runId=${encodeURIComponent(runId)}`,
    { headers: AUTH.viewer },
  );
  assert(result.status === 200, 'effect count failed', result);
  return result.body;
}

try {
  await serverLifecycle.preflight();
  console.log('> start workerd and drive a real model tool call to suspension');
  await launch();
  const actual = await startSuspended();
  const before = await effects(actual.runId);
  assert(
    before.modelCalls === 1 &&
      before.connectorCalls === 0 &&
      before.effects === 0,
    'write occurred before approval',
    before,
  );

  console.log('> kill workerd and restart on persisted D1');
  await stop();
  await launch();

  console.log('> public status and approval target survive the restart');
  const recovered = await waitSuspended(actual);
  assert(
    recovered.approval?.resumeTarget?.kind === 'agent-thread' &&
      recovered.approval.resumeTarget.agentId === AGENT_ID &&
      recovered.approval.resumeTarget.threadId === actual.threadId &&
      recovered.approval.resumeTarget.resourceId === actual.resourceId &&
      recovered.approval.resumeTarget.principal?.id === 'opal',
    'trusted resume target missing after restart',
    recovered.approval,
  );

  console.log('> the public agent surface exposes no raw resume route');
  const rawResume = await request(
    'POST',
    `/agents/${AGENT_ID}/runs/${encodeURIComponent(actual.threadId)}/${encodeURIComponent(actual.runId)}/resume`,
    {
      headers: AUTH.operator,
      body: { approved: true },
    },
  );
  assert(rawResume.status === 404, 'public raw resume did not fail closed', {
    rawResume,
    workerdOutput: workerdDiagnostics(),
  });
  const afterRawResume = await effects(actual.runId);
  assert(
    afterRawResume.connectorCalls === 0 && afterRawResume.effects === 0,
    'raw resume executed a write',
    afterRawResume,
  );

  const listed = await request('GET', '/api/approvals', {
    headers: AUTH.viewer,
  });
  const approval = listed.body.find(
    (record) => record.runId === actual.runId && record.status === 'pending',
  );
  assert(
    approval?.id === recovered.approval?.id &&
      approval.connectors?.length === 1 &&
      approval.connectors[0] === 'spike_recordWrite',
    'approval did not retain its exact connector capability',
    approval,
  );

  console.log(
    '> approve through the queue; a fresh thread DO restores the requester',
  );
  const decided = await request(
    'POST',
    `/api/approvals/${encodeURIComponent(approval.id)}/decide`,
    { headers: AUTH.reviewer, body: { decision: 'approve' } },
  );
  assert(
    decided.status === 200 &&
      decided.body.record?.decidedBy === 'ray' &&
      decided.body.resume?.ok === true &&
      decided.body.resume?.summary?.status === 'success',
    'approval resume failed',
    decided,
  );

  const finalStatus = await request(
    'GET',
    `/agents/${AGENT_ID}/runs/${encodeURIComponent(actual.threadId)}/${encodeURIComponent(actual.runId)}`,
    { headers: AUTH.viewer },
  );
  assert(
    finalStatus.status === 200 &&
      finalStatus.body.summary?.status === 'success' &&
      finalStatus.body.approval === undefined,
    'terminal status was not durably reconciled',
    finalStatus,
  );
  const after = await effects(actual.runId);
  const call = after.calls?.[0];
  assert(
    after.modelCalls === 1 &&
      after.connectorCalls === 1 &&
      after.effects === 1 &&
      call?.actor_id === 'opal' &&
      call?.actor_role === 'operator' &&
      call?.entry_path === 'approval.resume',
    'connector side effect was not exactly once under the original requester',
    after,
  );
  console.log(
    '\nLLM guarded-agent closeout passed: suspend → kill → approve → original principal → exactly-once write.',
  );
} finally {
  await serverLifecycle.cleanup(() => {
    rmSync(stateDir, { recursive: true, force: true });
  });
  server = undefined;
}
