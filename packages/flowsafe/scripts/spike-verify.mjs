// Automates the flowsafe workerd spike end-to-end so "durable execution
// across real process death" is a pass/fail command instead of a manual
// curl ritual (protocol: spike/worker.ts header). Phase A drives the guarded,
// catalog-hosted agent through authenticated start, suspension before its
// connector, a real workerd restart, approval by a different reviewer,
// restoration of the original requester principal, and exactly-once connector
// execution. Its negative matrix covers forged headers/context, disallowed
// roles, wrong agents/bindings, absent raw resume, stale decision replay,
// and restart-evicted stream replay with authoritative status fallback.
// The remaining scenarios retain the lower-level workflow, stream, background
// task, signal, goal, schedule, and webhook compatibility proofs.
//
// Auth rides the worker's host-kit seam: every request presents one of the
// LOCAL-ONLY spike bearer tokens (spike/worker.ts SPIKE_ACTORS). The C probe
// uses spike-admin because admin is the only role that can both START a run
// and DECIDE approvals — exactly the identity separation-of-duties must deny.
//
// Kill protocol: killing wrangler alone orphans its workerd child, which
// keeps serving the port and fakes persistence. So: capture descendant
// PIDs BEFORE the kill (orphans reparent to init after it), SIGKILL the
// whole process group, poll the PIDs dead, and PROVE the port refuses
// before restarting; `fuser -k <port>/tcp` (port-scoped — cannot touch
// interactive `pnpm spike` on 8787 or other repos' servers) is the last
// resort. Fault injection: SPIKE_VERIFY_FAULT=skip-decide skips the
// decide step so the harness's own failure path stays testable.
import { spawn } from 'node:child_process';
import { createHmac } from 'node:crypto';
import { createWriteStream, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { SignJWT } from 'jose';
import {
  createWorkerdServerLifecycle,
  parsePort,
} from '../../../scripts/workerd-server-lifecycle.mjs';

const FLOWSAFE = dirname(dirname(fileURLToPath(import.meta.url)));
const WRANGLER = join(FLOWSAFE, 'node_modules/.bin/wrangler');
const CONFIG = join(FLOWSAFE, 'spike/wrangler.jsonc');
const PORT = parsePort(
  process.env.SPIKE_VERIFY_PORT ?? 8799,
  'SPIKE_VERIFY_PORT',
);
const BASE = `http://127.0.0.1:${PORT}`;
const FAULT = process.env.SPIKE_VERIFY_FAULT || undefined;
const RUN_BODY = {
  workflowId: 'demo-approval',
  inputData: { topic: 'launch' },
};
// Track A: the agent tool-call gate suspends with the durable-agent approval
// shape (R-003) rather than an explicit `connectors` array.
const AGENT_RUN_BODY = {
  workflowId: 'demo-agent-gate',
  inputData: { topic: 'launch' },
};
const GUARDED_AGENT_ID = 'spike-guarded-agent';
const GUARDED_AGENT_START_PATH = `/agents/${GUARDED_AGENT_ID}/runs`;
const AUTH = {
  admin: { authorization: 'Bearer spike-admin' },
  operator: { authorization: 'Bearer spike-operator' },
  reviewer: { authorization: 'Bearer spike-reviewer' },
  viewer: { authorization: 'Bearer spike-viewer' },
  otherOperator: { authorization: 'Bearer other-operator' },
  otherReviewer: { authorization: 'Bearer other-reviewer' },
  otherViewer: { authorization: 'Bearer other-viewer' },
};

// Streaming (M-009). MUST equal spike/wrangler.jsonc `vars.STREAM_TICKET_SECRET`
// — startServer ALSO re-passes it via `--var` so the worker signs tickets with
// exactly this key. That lets the ticket probes below (a) prove a VALID forge is
// ACCEPTED (the positive control that makes the refusals meaningful — a secret
// mismatch would refuse everything and false-pass), and (b) craft EXPIRED and
// malformed tickets the worker must refuse at the CLAIM layer, not the
// signature. A LOCAL-ONLY spike fixture; never a real secret.
const STREAM_TICKET_SECRET = 'spike-local-stream-secret-do-not-deploy';
const DEPLOYMENT_TENANT = 'spike';
const DEPLOYMENT_IDENTITY_SECRET = 'spike-local-deployment-identity-secret';
const nowSec = () => Math.floor(Date.now() / 1000);

// Track E (M-007): the github webhook signing secret. MUST equal
// spike/wrangler.jsonc `vars.GITHUB_WEBHOOK_SECRET`; startServer re-passes it via
// `--var` so the worker verifies with exactly this key, letting a VALID-signed
// webhook be ACCEPTED (the positive control) and a forged one REJECTED at the
// signature. A LOCAL-ONLY spike fixture; never a real secret.
const GITHUB_WEBHOOK_SECRET = 'spike-local-github-secret-do-not-deploy';
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

let currentStep = 'startup';
let currentServer;
let tmpDir;
const servers = [];
// Every client WebSocket opened by the stream probes, closed in cleanup() so a
// lingering socket cannot hold the process open after the run.
const clientSockets = [];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function assert(condition, label, detail) {
  if (condition) return;
  const suffix =
    detail === undefined ? '' : `\n  got: ${JSON.stringify(detail)}`;
  throw new Error(`${label}${suffix}`);
}

async function step(label, fn) {
  currentStep = label;
  const startedAt = Date.now();
  console.log(`\n> ${label}`);
  const result = await fn();
  const seconds = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(`  ok (${seconds}s)`);
  return result;
}

function startServer(generation, stateDir, logPath) {
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
      '--var',
      `DEPLOYMENT_TENANT:${DEPLOYMENT_TENANT}`,
      '--var',
      `DEPLOYMENT_IDENTITY_SECRET:${DEPLOYMENT_IDENTITY_SECRET}`,
      // Sign stream tickets with the SAME key spike-verify forges with, so the
      // ticket fail-closed probes exercise CLAIM rejection, not signature drift.
      '--var',
      `STREAM_TICKET_SECRET:${STREAM_TICKET_SECRET}`,
      // Track E: verify github webhooks with the SAME secret spike-verify signs
      // with, so a valid webhook is ACCEPTED and a forged one REJECTED (E-S2).
      '--var',
      `GITHUB_WEBHOOK_SECRET:${GITHUB_WEBHOOK_SECRET}`,
    ],
    {
      cwd: FLOWSAFE,
      detached: true, // own process group so the kill takes the whole tree
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

function executeLocalD1(stateDir, command) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      WRANGLER,
      [
        'd1',
        'execute',
        'flowsafe-demo',
        '--local',
        '--yes',
        '--config',
        CONFIG,
        '--persist-to',
        stateDir,
        '--command',
        command,
      ],
      {
        cwd: FLOWSAFE,
        env: { ...process.env, WRANGLER_SEND_METRICS: 'false', NO_COLOR: '1' },
        stdio: ['ignore', 'pipe', 'pipe'],
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
      if (code === 0) {
        resolve();
        return;
      }
      reject(
        new Error(
          `wrangler d1 execute failed (${signal ?? `exit ${code}`}): ${output.slice(-2000)}`,
        ),
      );
    });
  });
}

const serverLifecycle = createWorkerdServerLifecycle({ port: PORT });
const portState = () => serverLifecycle.portState();

async function launchServer(generation, stateDir, logPath) {
  currentServer = await serverLifecycle.start(
    generation,
    () => startServer(generation, stateDir, logPath),
    90_000,
  );
  return currentServer;
}

async function killServer(server) {
  if (server !== currentServer || server !== serverLifecycle.activeServer) {
    throw new Error('refusing to stop a server that is not the active server');
  }
  await serverLifecycle.stop();
  currentServer = undefined;
}

async function http(method, path, { body, headers } = {}) {
  return serverLifecycle.requestJson(
    (recoverySignal) =>
      fetch(`${BASE}${path}`, {
        method,
        headers: {
          ...(body === undefined ? {} : { 'content-type': 'application/json' }),
          ...headers,
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: recoverySignal ?? AbortSignal.timeout(30_000),
      }),
    {
      requestLabel: `${method} ${path}`,
      replaySafe: method === 'GET',
    },
  );
}

async function httpRaw(method, path, rawBody, headers = {}) {
  const response = await fetch(`${BASE}${path}`, {
    method,
    headers: {
      'content-type': 'application/json',
      ...headers,
    },
    body: rawBody,
    signal: AbortSignal.timeout(30_000),
  });
  const text = await response.text();
  try {
    return { status: response.status, body: JSON.parse(text) };
  } catch {
    throw new Error(
      `${method} ${path} -> ${response.status} non-JSON: ${text.slice(0, 300)}`,
    );
  }
}

function guardedStatusPath(run) {
  return `/agents/${GUARDED_AGENT_ID}/runs/${encodeURIComponent(run.threadId)}/${encodeURIComponent(run.runId)}`;
}

// --- Track E (M-007) webhook probe helpers ---------------------------------

// GitHub's X-Hub-Signature-256 = 'sha256=' + hex HMAC-SHA256(secret, rawBody).
// crypto.createHmac(...).digest('hex') is byte-equal to the worker's WebCrypto
// verify, so a VALID signature is ACCEPTED and a forged one REJECTED.
function githubSign(secret, rawBody) {
  return `sha256=${createHmac('sha256', secret).update(rawBody).digest('hex')}`;
}

// POST a webhook with the RAW body the signature is computed over (never a
// re-stringify — the signature must cover the exact bytes sent). `forge` presents
// a valid-LENGTH wrong signature (32 zero bytes), so the reject is at the
// signature verify, not a length short-circuit.
async function postWebhook(providerId, payload, { forge } = {}) {
  const raw = JSON.stringify(payload);
  const signature = forge
    ? `sha256=${'0'.repeat(64)}`
    : githubSign(GITHUB_WEBHOOK_SECRET, raw);
  const response = await fetch(
    `${BASE}/api/signal-providers/${providerId}/webhook`,
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-hub-signature-256': signature,
      },
      body: raw,
      signal: AbortSignal.timeout(30_000),
    },
  );
  const text = await response.text();
  try {
    return { status: response.status, body: JSON.parse(text) };
  } catch {
    return { status: response.status, body: text };
  }
}

// --- Live-stream (WebSocket) probe helpers (M-009) -------------------------

// Forge arbitrary claims with the same standard JWT envelope as
// mintStreamTicket. A valid forge proves the shared fixture secret matches the
// Worker; expired and address-mismatched forges then exercise claim rejection.
function forgeTicket(claims) {
  return new SignJWT({ ...claims, aud: 'flowsafe-stream' })
    .setProtectedHeader({ alg: 'HS256', typ: 'flowsafe-stream-ticket+jwt' })
    .sign(new TextEncoder().encode(STREAM_TICKET_SECRET));
}

// Mint a ticket the LEGITIMATE way: over the authenticated REST route.
async function mintTicketApi(channel, runId, headers) {
  const body =
    runId === undefined
      ? { channel }
      : { channel, runId, workflowId: 'demo-approval' };
  const { status, body: res } = await http('POST', '/api/stream/ticket', {
    body,
    headers,
  });
  assert(status === 200, `mint ${channel} ticket -> ${status}`, res);
  assert(typeof res.ticket === 'string', 'ticket is a string', res);
  assert(typeof res.url === 'string', 'ticket url is a string', res);
  return res; // { url, ticket, expiresAt }
}

function wsUrl(path, ticket) {
  const sep = path.includes('?') ? '&' : '?';
  return `ws://127.0.0.1:${PORT}${path}${sep}ticket=${encodeURIComponent(ticket)}`;
}

// Open a subscription and collect parsed frames. Tracked for cleanup.
function connectWs(path, ticket) {
  const ws = new WebSocket(wsUrl(path, ticket));
  clientSockets.push(ws);
  const frames = [];
  const listeners = new Set();
  ws.addEventListener('message', (event) => {
    const text =
      typeof event.data === 'string' ? event.data : String(event.data);
    let parsed = null;
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = null; // a non-JSON frame is ignored by the predicates below
    }
    frames.push(parsed);
    for (const fn of [...listeners]) fn(parsed);
  });
  return {
    ws,
    frames,
    onFrame(fn) {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },
    close() {
      try {
        ws.close();
      } catch {
        // already closing/closed
      }
    },
  };
}

function waitOpen(conn, timeoutMs) {
  return new Promise((resolve, reject) => {
    if (conn.ws.readyState === WebSocket.OPEN) {
      resolve();
      return;
    }
    const timer = setTimeout(
      () => reject(new Error('websocket did not open in time')),
      timeoutMs,
    );
    conn.ws.addEventListener(
      'open',
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
    conn.ws.addEventListener(
      'error',
      () => {
        clearTimeout(timer);
        reject(new Error('websocket errored before open'));
      },
      { once: true },
    );
    conn.ws.addEventListener(
      'close',
      (event) => {
        clearTimeout(timer);
        reject(new Error(`websocket closed before open (code ${event.code})`));
      },
      { once: true },
    );
  });
}

function waitFrame(conn, predicate, timeoutMs) {
  const existing = conn.frames.find((frame) => predicate(frame));
  if (existing !== undefined) return Promise.resolve(existing);
  return new Promise((resolve, reject) => {
    let off = () => {};
    const timer = setTimeout(() => {
      off();
      reject(new Error('expected websocket frame not received in time'));
    }, timeoutMs);
    off = conn.onFrame((frame) => {
      if (predicate(frame)) {
        clearTimeout(timer);
        off();
        resolve(frame);
      }
    });
  });
}

// A ticket that MUST be refused: OPENING is a LEAK (reject). A handshake error
// or a close (the worker returned a non-101, e.g. 403) is the pass. A timeout
// with neither is a FAILURE (ambiguous — never a silent pass).
function expectWsRefused(path, ticket, timeoutMs) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl(path, ticket));
    clientSockets.push(ws);
    let timer;
    let settled = false;
    const settle = (fn, arg) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        ws.close();
      } catch {
        // already closing/closed
      }
      fn(arg);
    };
    timer = setTimeout(
      () =>
        settle(
          reject,
          new Error('websocket neither opened nor was refused (ambiguous)'),
        ),
      timeoutMs,
    );
    ws.addEventListener('open', () =>
      settle(
        reject,
        new Error('LEAK: websocket opened on a ticket that must be refused'),
      ),
    );
    ws.addEventListener('error', () => settle(resolve));
    ws.addEventListener('close', () => settle(resolve));
  });
}

function closeClientSockets() {
  for (const ws of clientSockets.splice(0)) {
    try {
      ws.close();
    } catch {
      // already closing/closed
    }
  }
}

function dumpLog(server) {
  const tail = server.chunks.join('').split('\n').slice(-200);
  console.error(`\n--- ${server.generation} log tail (${server.logPath}) ---`);
  console.error(tail.join('\n'));
}

// Even when the kill fails (port won't free), release what we own —
// log fds and the temp dir — before the failure propagates.
async function cleanup() {
  closeClientSockets();
  await serverLifecycle.cleanup(() => {
    for (const server of servers) {
      server.file.end();
    }
    if (tmpDir !== undefined) {
      rmSync(tmpDir, { recursive: true, force: true });
      tmpDir = undefined;
    }
  });
  currentServer = undefined;
}

async function main() {
  if (FAULT !== undefined && FAULT !== 'skip-decide') {
    throw new Error(
      `unknown SPIKE_VERIFY_FAULT '${FAULT}' (supported: skip-decide)`,
    );
  }

  await step(`preflight: port ${PORT} must be free`, async () => {
    await serverLifecycle.preflight();
  });

  tmpDir = mkdtempSync(join(tmpdir(), 'spike-verify-'));
  const stateDir = join(tmpDir, 'state');

  await step('provision deployment identity sentinel', () =>
    executeLocalD1(
      stateDir,
      "CREATE TABLE IF NOT EXISTS flowsafe_deployment (id INTEGER PRIMARY KEY CHECK (id = 1), tenant_tag TEXT NOT NULL, provisioned_at TEXT NOT NULL); INSERT OR IGNORE INTO flowsafe_deployment (id, tenant_tag, provisioned_at) VALUES (1, 'spike', datetime('now'));",
    ),
  );

  await step('gen-1: server ready', () =>
    launchServer('gen-1', stateDir, join(tmpDir, 'gen1.log')),
  );

  await step(
    'PA0 boundary: authenticated catalog, role gates, and forged context/header rejection',
    async () => {
      const missing = await http('GET', '/agents');
      assert(
        missing.status === 401,
        'agent catalog requires bearer authentication',
        missing,
      );

      const catalog = await http('GET', '/agents', {
        headers: AUTH.viewer,
      });
      assert(
        catalog.status === 200,
        'authenticated catalog is readable',
        catalog,
      );
      assert(
        catalog.body.actor?.id === 'vic' &&
          catalog.body.actor?.role === 'viewer' &&
          catalog.body.actor?.tenantId === undefined,
        'catalog echoes the authenticated actor',
        catalog.body.actor,
      );
      assert(
        catalog.body.agents?.length === 1 &&
          catalog.body.agents[0]?.id === GUARDED_AGENT_ID,
        'catalog exposes the registered guarded agent metadata',
        catalog.body.agents,
      );

      const forgedHeader = await http('GET', '/agents', {
        headers: {
          ...AUTH.operator,
          'X-FloWsAfE-RoLe': 'admin',
        },
      });
      assert(
        forgedHeader.status === 403,
        'a mixed-case forged server role header is refused before scoping',
        forgedHeader,
      );

      for (const [label, headers] of [
        ['viewer', AUTH.viewer],
        ['reviewer', AUTH.reviewer],
      ]) {
        const denied = await http('POST', GUARDED_AGENT_START_PATH, {
          headers,
          body: { prompt: 'Call the required write tool.' },
        });
        assert(
          denied.status === 403,
          `${label} cannot mutate the guarded agent`,
          denied,
        );
      }

      const unknownBeforeRole = await http(
        'POST',
        '/agents/not-registered/runs',
        {
          headers: AUTH.viewer,
          body: { prompt: 'must not run' },
        },
      );
      assert(
        unknownBeforeRole.status === 404,
        'unknown agent stays 404 before mutation-role disclosure',
        unknownBeforeRole,
      );

      for (const forbiddenBody of [
        {
          prompt: 'must not run',
          inputProcessors: [],
        },
        {
          prompt: 'must not run',
          requestContext: { 'breakwater.actor': { id: 'mallory' } },
        },
        { prompt: 'must not run', runId: 'spike_forged' },
        { prompt: 'must not run', threadId: 'spike_forged' },
      ]) {
        const rejected = await http('POST', GUARDED_AGENT_START_PATH, {
          headers: AUTH.operator,
          body: forbiddenBody,
        });
        assert(
          rejected.status === 400,
          'processor/context/id overrides are rejected at the HTTP boundary',
          { forbiddenBody, rejected },
        );
      }

      const prototypeKey = await httpRaw(
        'POST',
        GUARDED_AGENT_START_PATH,
        '{"prompt":"must not run","__proto__":{"polluted":true}}',
        AUTH.operator,
      );
      assert(
        prototypeKey.status === 400,
        'prototype meta-keys are rejected',
        prototypeKey,
      );

      const untouched = await http('GET', '/agent/live/effects', {
        headers: AUTH.viewer,
      });
      assert(
        untouched.status === 200 &&
          untouched.body.modelCalls === 0 &&
          untouched.body.inputProcessorCalls === 0 &&
          untouched.body.connectorCalls === 0 &&
          untouched.body.effects === 0,
        'no rejected boundary probe reached the input processor, model, or connector',
        untouched,
      );
    },
  );

  const guardedRun = await step(
    'PA1 guarded start: server-minted run suspends before the connector',
    async () => {
      const started = await http('POST', GUARDED_AGENT_START_PATH, {
        headers: AUTH.operator,
        body: { prompt: 'Call the required write tool exactly once.' },
      });
      assert(started.status === 200, 'guarded agent start succeeded', started);
      const envelope = started.body;
      assert(
        envelope.agentId === GUARDED_AGENT_ID,
        'start envelope identifies the catalog agent',
        envelope,
      );
      assert(
        UUID_PATTERN.test(envelope.threadId) &&
          envelope.resourceId === envelope.threadId &&
          UUID_PATTERN.test(envelope.runId),
        'thread and run IDs are server-minted; the resource ID is a trusted, validated key',
        envelope,
      );
      assert(
        envelope.summary?.status === 'suspended',
        'guarded run suspended at the connector gate',
        envelope.summary,
      );
      assert(
        envelope.approval?.status === 'pending' &&
          envelope.approval?.connectors?.includes('spike_recordWrite'),
        'suspension filed the exact connector approval',
        envelope.approval,
      );
      assert(
        envelope.approval?.requestedBy === 'opal' &&
          envelope.approval?.resumeTarget?.kind === 'agent-thread' &&
          envelope.approval.resumeTarget.agentId === GUARDED_AGENT_ID &&
          envelope.approval.resumeTarget.threadId === envelope.threadId &&
          envelope.approval.resumeTarget.resourceId === envelope.resourceId &&
          envelope.approval.resumeTarget.principal?.id === 'opal' &&
          envelope.approval.resumeTarget.principal?.role === 'operator' &&
          envelope.approval.resumeTarget.principal?.tenantId === undefined,
        'approval target durably carries the original authorized principal and binding',
        envelope.approval,
      );

      const effects = await http(
        'GET',
        `/agent/live/effects?runId=${encodeURIComponent(envelope.runId)}`,
        { headers: AUTH.viewer },
      );
      assert(
        effects.body.modelCalls === 1 &&
          effects.body.inputProcessorCalls === 1 &&
          JSON.stringify(effects.body.inputProcessorMessageCounts) === '[1]' &&
          effects.body.connectorCalls === 0 &&
          effects.body.effects === 0,
        'the non-empty input ran once and the connector did not run before approval',
        effects.body,
      );

      const rawResume = await http(
        'POST',
        `${guardedStatusPath(envelope)}/resume`,
        {
          headers: AUTH.operator,
          body: { approved: true, decidedBy: 'mallory' },
        },
      );
      assert(
        rawResume.status === 404,
        'the public agent surface has no raw resume route',
        rawResume,
      );
      return {
        agentId: envelope.agentId,
        threadId: envelope.threadId,
        resourceId: envelope.resourceId,
        runId: envelope.runId,
        approvalId: envelope.approval.id,
      };
    },
  );

  const run = await step(
    'A1 start: run suspends at approval gate',
    async () => {
      const { status, body } = await http('POST', '/runs', {
        body: RUN_BODY,
        headers: AUTH.operator,
      });
      assert(status === 200, `POST /runs -> ${status}`, body);
      assert(body.status === 'suspended', 'run suspended', body.status);
      assert(typeof body.runId === 'string', 'runId is a string', body.runId);
      assert(
        UUID_PATTERN.test(body.runId),
        'runId is a server-minted opaque UUID',
        body.runId,
      );
      assert(
        JSON.stringify(body.suspended?.[0]) === JSON.stringify(['approval']),
        "suspended[0] is ['approval']",
        body.suspended,
      );
      assert(
        body.suspendPayload?.approval?.reason ===
          'human approval required before publish',
        'suspend reason',
        body.suspendPayload,
      );
      assert(
        typeof body.approval?.id === 'string',
        'approval id',
        body.approval,
      );
      assert(
        body.approval.status === 'pending',
        'approval pending',
        body.approval.status,
      );
      return { runId: body.runId, approvalId: body.approval.id };
    },
  );

  await step('A2 kill: group-SIGKILL gen-1, prove port refused', async () => {
    await killServer(currentServer);
  });

  await step('A2 restart: gen-2 restored state ready', async () => {
    await launchServer('gen-2', stateDir, join(tmpDir, 'gen2.log'));
    assert(
      !/address already in use/i.test(currentServer.chunks.join('')),
      'gen-2 log must not contain "address already in use" (orphan trap)',
    );
    const recovered = await http('GET', guardedStatusPath(guardedRun), {
      headers: AUTH.viewer,
    });
    assert(
      recovered.status === 200 &&
        recovered.body.summary?.status === 'suspended' &&
        recovered.body.approval?.id === guardedRun.approvalId,
      'gen-2 restored the authoritative guarded status path',
      recovered,
    );
  });

  await step(
    'PA2 restart: replay cache is gone, authoritative status and principal survive',
    async () => {
      const replay = await http(
        'GET',
        `${guardedStatusPath(guardedRun)}/stream?offset=0`,
        { headers: AUTH.viewer },
      );
      assert(
        replay.status === 409 &&
          String(replay.body.error ?? '').includes('status'),
        'restart-evicted stream replay returns 409 with status fallback',
        replay,
      );

      const status = await http('GET', guardedStatusPath(guardedRun), {
        headers: AUTH.viewer,
      });
      assert(
        status.status === 200 &&
          status.body.summary?.status === 'suspended' &&
          status.body.agentId === GUARDED_AGENT_ID &&
          status.body.threadId === guardedRun.threadId &&
          status.body.resourceId === guardedRun.resourceId &&
          status.body.runId === guardedRun.runId,
        'authoritative durable status survives the workerd restart',
        status,
      );
      assert(
        status.body.approval?.id === guardedRun.approvalId &&
          status.body.approval?.resumeTarget?.principal?.id === 'opal' &&
          status.body.approval?.resumeTarget?.principal?.role === 'operator',
        'status reconciliation retained the original execution principal',
        status.body.approval,
      );

      const listed = await http('GET', '/api/approvals', {
        headers: AUTH.viewer,
      });
      const record = listed.body.find(
        (candidate) => candidate.id === guardedRun.approvalId,
      );
      assert(
        listed.status === 200 &&
          record?.status === 'pending' &&
          record?.resumeTarget?.kind === 'agent-thread' &&
          record?.resumeTarget?.agentId === GUARDED_AGENT_ID &&
          record?.resumeTarget?.principal?.id === 'opal' &&
          record?.resumeTarget?.principal?.role === 'operator' &&
          record?.resumeTarget?.principal?.tenantId === undefined,
        'D1 approval record survived with its agent binding and principal',
        record,
      );

      const effects = await http(
        'GET',
        `/agent/live/effects?runId=${encodeURIComponent(guardedRun.runId)}`,
        { headers: AUTH.viewer },
      );
      assert(
        effects.status === 200 &&
          effects.body.inputProcessorCalls === 1 &&
          JSON.stringify(effects.body.inputProcessorMessageCounts) === '[1]',
        'the persisted input processor invocation survived the restart exactly once',
        effects.body,
      );

      const peer = await http('GET', guardedStatusPath(guardedRun), {
        headers: AUTH.otherViewer,
      });
      assert(
        peer.status === 200,
        'another authenticated viewer in the deployment can read the shared run',
        peer,
      );
      const wrongAgent = await http(
        'GET',
        guardedStatusPath(guardedRun).replace(
          `/agents/${GUARDED_AGENT_ID}/`,
          '/agents/not-registered/',
        ),
        { headers: AUTH.viewer },
      );
      assert(
        wrongAgent.status === 404,
        'a mismatched agent id is not found',
        wrongAgent,
      );
    },
  );

  await step(
    'PA3 approval resume: reviewer differs, requester principal restored, connector exactly once',
    async () => {
      const decided = await http(
        'POST',
        `/api/approvals/${guardedRun.approvalId}/decide`,
        {
          headers: AUTH.reviewer,
          body: { decision: 'approve' },
        },
      );
      assert(
        decided.status === 200 &&
          decided.body.record?.decidedBy === 'ray' &&
          decided.body.resume?.attempted === true &&
          decided.body.resume?.ok === true &&
          decided.body.resume?.summary?.status === 'success',
        'a different authorized reviewer resumed the guarded run to success',
        decided,
      );

      const finalStatus = await http('GET', guardedStatusPath(guardedRun), {
        headers: AUTH.viewer,
      });
      assert(
        finalStatus.status === 200 &&
          finalStatus.body.summary?.status === 'success' &&
          finalStatus.body.approval === undefined &&
          finalStatus.body.approvals === undefined,
        'terminal status is authoritative and omits suspension-only approvals',
        finalStatus,
      );

      const effects = await http(
        'GET',
        `/agent/live/effects?runId=${encodeURIComponent(guardedRun.runId)}`,
        { headers: AUTH.viewer },
      );
      const call = effects.body.calls?.[0];
      assert(
        effects.status === 200 &&
          effects.body.modelCalls === 1 &&
          effects.body.inputProcessorCalls === 1 &&
          JSON.stringify(effects.body.inputProcessorMessageCounts) === '[1]' &&
          effects.body.connectorCalls === 1 &&
          effects.body.effects === 1,
        'resume did not replay application input and the connector executed exactly once',
        effects.body,
      );
      assert(
        call?.run_id === guardedRun.runId &&
          call?.thread_id === guardedRun.threadId &&
          call?.resource_id === guardedRun.resourceId &&
          call?.actor_id === 'opal' &&
          call?.actor_role === 'operator' &&
          call?.deployment_tag === 'spike' &&
          call?.entry_path === 'approval.resume',
        'the connector saw the original requester and infrastructure deployment tag',
        call,
      );

      const staleDecision = await http(
        'POST',
        `/api/approvals/${guardedRun.approvalId}/decide`,
        {
          headers: AUTH.reviewer,
          body: { decision: 'approve' },
        },
      );
      assert(
        staleDecision.status === 409,
        'a stale/replayed approval decision cannot mint another execution leg',
        staleDecision,
      );
      const afterReplay = await http(
        'GET',
        `/agent/live/effects?runId=${encodeURIComponent(guardedRun.runId)}`,
        { headers: AUTH.viewer },
      );
      assert(
        afterReplay.body.inputProcessorCalls === 1 &&
          afterReplay.body.connectorCalls === 1 &&
          afterReplay.body.effects === 1,
        'stale approval replay did not rerun input processing or the connector',
        afterReplay.body,
      );
    },
  );

  await step(
    'PA4 binding mismatch: a run cannot be read through another bound thread',
    async () => {
      const second = await http('POST', GUARDED_AGENT_START_PATH, {
        headers: AUTH.operator,
        body: { prompt: 'Call the required write tool exactly once.' },
      });
      assert(
        second.status === 200 && second.body.summary?.status === 'suspended',
        'second guarded run suspended',
        second,
      );
      const mismatched = await http(
        'GET',
        `/agents/${GUARDED_AGENT_ID}/runs/${encodeURIComponent(guardedRun.threadId)}/${encodeURIComponent(second.body.runId)}`,
        { headers: AUTH.viewer },
      );
      assert(
        mismatched.status === 404,
        'stored thread/run binding mismatch returns 404',
        mismatched,
      );
    },
  );

  await step('A3 list: approval survived the restart', async () => {
    const { status, body } = await http('GET', '/api/approvals', {
      headers: AUTH.viewer,
    });
    assert(status === 200, `GET /api/approvals -> ${status}`, body);
    assert(Array.isArray(body), 'list is a bare array', body);
    const record = body.find((entry) => entry.id === run.approvalId);
    assert(record !== undefined, 'record with captured id exists', body);
    assert(record.status === 'pending', 'record pending', record.status);
    assert(record.runId === run.runId, 'record runId matches', record.runId);
  });

  if (FAULT === 'skip-decide') {
    console.log('\n! SPIKE_VERIFY_FAULT=skip-decide: skipping A4 decide');
  } else {
    await step('A4 decide: approve -> grant-minted resume', async () => {
      const { status, body } = await http(
        'POST',
        `/api/approvals/${run.approvalId}/decide`,
        {
          headers: AUTH.reviewer,
          body: { decision: 'approve' },
        },
      );
      assert(status === 200, `decide -> ${status}`, body);
      assert(body.resume?.attempted === true, 'resume attempted', body.resume);
      assert(
        body.resume?.ok === true,
        'resume ok (HTTP 200 does NOT imply this)',
        body.resume,
      );
      assert(
        body.resume?.summary?.status === 'success',
        'resumed run succeeded',
        body.resume?.summary,
      );
      assert(
        body.resume?.summary?.result?.published === true,
        'resumed run published',
        body.resume?.summary?.result,
      );
    });
  }

  await step('A5 final: run success persisted', async () => {
    const { status, body } = await http(
      'GET',
      `/runs/demo-approval/${run.runId}`,
      { headers: AUTH.viewer },
    );
    assert(status === 200, `final status -> ${status}`, body);
    assert(body.status === 'success', 'final run status', body.status);
    assert(body.result?.published === true, 'published', body.result);
    assert(body.result?.approvedBy === 'ray', 'approvedBy', body.result);
  });

  await step('B forged-resume: no grant -> fails closed', async () => {
    const started = await http('POST', '/runs', {
      body: RUN_BODY,
      headers: AUTH.operator,
    });
    assert(
      started.status === 200 && started.body.status === 'suspended',
      'second run suspended',
      { status: started.status, body: started.body },
    );
    const forged = await http(
      'POST',
      `/runs/demo-approval/${started.body.runId}/resume`,
      {
        headers: AUTH.operator,
        body: {
          step: started.body.suspended[0],
          resumeData: { approved: true, decidedBy: 'mallory' },
        },
      },
    );
    assert(
      forged.status === 200,
      `forged resume -> HTTP ${forged.status} (DO always json()s the summary)`,
      forged.body,
    );
    assert(
      forged.body.status === 'failed',
      'forged resume failed closed',
      forged.body,
    );
    assert(
      String(forged.body.error ?? '').includes(
        'approval required and no matching structured grant was found',
      ),
      'gate error names the missing grant',
      forged.body.error,
    );
  });

  await step(
    'C self-decision: requester cannot decide their own run -> denied',
    async () => {
      // Start a run AS the admin, so ada is recorded as the approval's
      // requester (the run router attributes requestedBy from the
      // authenticated actor). ada then trying to decide their own run must
      // hit the separation-of-duties denial — the bridge must NOT attribute
      // the request to the system principal. admin is the only role that can
      // both start (RUN_START_ROLES) and decide, so it is the only identity
      // this probe CAN use under the shared router's coarse gate.
      const started = await http('POST', '/runs', {
        headers: AUTH.admin,
        body: RUN_BODY,
      });
      assert(
        started.status === 200 && started.body.status === 'suspended',
        'self-decision run suspended',
        { status: started.status, body: started.body },
      );
      assert(
        started.body.approval?.requestedBy === 'ada',
        'approval attributed to the starting actor (requestedBy=ada)',
        started.body.approval,
      );
      const selfDecide = await http(
        'POST',
        `/api/approvals/${started.body.approval.id}/decide`,
        {
          headers: AUTH.admin,
          body: { decision: 'approve' },
        },
      );
      assert(
        selfDecide.status === 403,
        `self-decision denied -> HTTP ${selfDecide.status} (expected 403)`,
        selfDecide.body,
      );
      assert(
        String(selfDecide.body.error ?? '').includes('separation of duties'),
        'denial cites separation of duties',
        selfDecide.body.error,
      );
    },
  );

  // --- Track A agent gate (M-002): the durable-agent approval-suspend shape --
  // (R-003) round-trips through the SAME grant-only path on workerd + D1. The
  // gate suspends with { type:'approval', toolName } and NO explicit
  // `connectors`, so this proves host-kit's bridge derives connectors:[toolName]
  // from the agent shape (S2/round-trip), and a forged resume of it fails closed
  // at the same grant gate (S5).
  const agentRun = await step(
    'AG1 agent gate: start -> suspends with the agent shape, connector derived',
    async () => {
      const { status, body } = await http('POST', '/runs', {
        body: AGENT_RUN_BODY,
        headers: AUTH.operator,
      });
      assert(status === 200, `POST /runs (agent) -> ${status}`, body);
      assert(body.status === 'suspended', 'agent run suspended', body.status);
      assert(
        JSON.stringify(body.suspended?.[0]) === JSON.stringify(['agent-gate']),
        "suspended[0] is ['agent-gate']",
        body.suspended,
      );
      // The AGENT suspend shape, not a workflow `connectors` array.
      assert(
        body.suspendPayload?.['agent-gate']?.type === 'approval' &&
          body.suspendPayload?.['agent-gate']?.toolName === 'demo-publisher',
        'suspend payload is the agent tool-call shape (type/toolName)',
        body.suspendPayload,
      );
      // R-003: the bridge derived the connector to grant FROM the agent shape.
      assert(
        Array.isArray(body.approval?.connectors) &&
          body.approval.connectors.includes('demo-publisher'),
        'approval connectors derived from toolName (R-003)',
        body.approval,
      );
      return { runId: body.runId, approvalId: body.approval.id };
    },
  );

  await step(
    'AG2 agent gate: approve -> grant round-trips -> run publishes',
    async () => {
      const { status, body } = await http(
        'POST',
        `/api/approvals/${agentRun.approvalId}/decide`,
        { headers: AUTH.reviewer, body: { decision: 'approve' } },
      );
      assert(status === 200, `decide (agent) -> ${status}`, body);
      assert(
        body.resume?.summary?.status === 'success',
        'agent run resumed to success (engine-leg grant reached the connector)',
        body.resume?.summary,
      );
      assert(
        body.resume?.summary?.result?.published === true,
        'agent run published under the derived grant',
        body.resume?.summary?.result,
      );
    },
  );

  await step(
    'AG3 agent gate forged-resume: no grant -> fails closed',
    async () => {
      const started = await http('POST', '/runs', {
        body: AGENT_RUN_BODY,
        headers: AUTH.operator,
      });
      assert(
        started.status === 200 && started.body.status === 'suspended',
        'forged-probe agent run suspended',
        { status: started.status, body: started.body },
      );
      const forged = await http(
        'POST',
        `/runs/demo-agent-gate/${started.body.runId}/resume`,
        {
          headers: AUTH.operator,
          body: {
            step: started.body.suspended[0],
            resumeData: { approved: true, decidedBy: 'mallory' },
          },
        },
      );
      assert(
        forged.status === 200 && forged.body.status === 'failed',
        'forged agent resume failed closed',
        forged.body,
      );
      assert(
        String(forged.body.error ?? '').includes(
          'approval required and no matching structured grant was found',
        ),
        'agent gate error names the missing grant',
        forged.body.error,
      );
    },
  );

  // --- Part B live streaming (M-009): real WebSockets over workerd ----------
  // Each probe below opens an ACTUAL WebSocket against wrangler dev and asserts
  // something that can FAIL (and exit non-zero): a fanned-out event is received,
  // the deployment hub reaches multiple actors, a subscription survives a
  // kill+restart, and expired/malformed/garbage/cross-channel tickets are refused.

  await step(
    'D deployment fan-out: a decided event reaches two authenticated actors on ' +
      'the singleton deployment hub',
    async () => {
      // A fresh run -> a pending approval (requestedBy=opal) ray decides below.
      const started = await http('POST', '/runs', {
        body: RUN_BODY,
        headers: AUTH.operator,
      });
      assert(
        started.status === 200 && started.body.status === 'suspended',
        'D run suspended',
        { status: started.status, body: started.body },
      );
      const approvalId = started.body.approval?.id;
      assert(typeof approvalId === 'string', 'D approval id', started.body);

      // Subscribe the spike hub over a LEGITIMATELY minted ticket.
      const spikeTicket = await mintTicketApi('hub', undefined, AUTH.viewer);
      const spike = connectWs(spikeTicket.url, spikeTicket.ticket);
      await waitOpen(spike, 10_000);
      // The hub broadcasts presence on connect — a subscription barrier proving
      // acceptWebSocket registered this socket before the mutation fans out.
      await waitFrame(spike, (frame) => frame?.type === 'presence', 10_000);

      const peerTicket = await mintTicketApi(
        'hub',
        undefined,
        AUTH.otherViewer,
      );
      const peer = connectWs(peerTicket.url, peerTicket.ticket);
      await waitOpen(peer, 10_000);
      await waitFrame(peer, (frame) => frame?.type === 'presence', 10_000);

      // Decide the approval -> fires a 'decided' event over the deployment hub.
      const decided = await http(
        'POST',
        `/api/approvals/${approvalId}/decide`,
        { headers: AUTH.reviewer, body: { decision: 'approve' } },
      );
      assert(decided.status === 200, 'D decide ok', decided.body);

      // Fan-out: the spike socket receives the decided event.
      const frame = await waitFrame(
        spike,
        (candidate) =>
          candidate?.type === 'queue' && candidate?.event?.type === 'decided',
        10_000,
      );
      assert(
        frame.event.record.tenantId === undefined,
        'fanned-out approval records carry no logical tenant field',
        frame.event.record,
      );
      assert(
        frame.event.record.id === approvalId,
        'fanned-out event is the decided approval',
        frame.event.record,
      );

      const peerFrame = await waitFrame(
        peer,
        (candidate) =>
          candidate?.type === 'queue' && candidate?.event?.type === 'decided',
        10_000,
      );
      assert(
        peerFrame.event.record.id === approvalId,
        'the second deployment actor received the same decided event',
        peerFrame,
      );

      spike.close();
      peer.close();
    },
  );

  await step(
    'E hibernation persistence: a re-opened subscription still receives ' +
      'events across a workerd kill+restart',
    async () => {
      await killServer(currentServer);
      await launchServer('gen-3', stateDir, join(tmpDir, 'gen3.log'));
      assert(
        !/address already in use/i.test(currentServer.chunks.join('')),
        'gen-3 log must not contain "address already in use" (orphan trap)',
      );

      // A fresh run + subscription on the RESTARTED process; a decided event
      // still reaches the reconnected socket -> the WS subscription is not lost
      // across a DO eviction/process restart (the hub re-binds via idFromName
      // and fans out over the hibernatable-WebSocket API).
      const started = await http('POST', '/runs', {
        body: RUN_BODY,
        headers: AUTH.operator,
      });
      assert(
        started.status === 200 && started.body.status === 'suspended',
        'E run suspended',
        { status: started.status, body: started.body },
      );
      const approvalId = started.body.approval?.id;
      assert(typeof approvalId === 'string', 'E approval id', started.body);

      const ticket = await mintTicketApi('hub', undefined, AUTH.viewer);
      const sock = connectWs(ticket.url, ticket.ticket);
      await waitOpen(sock, 10_000);
      await waitFrame(sock, (frame) => frame?.type === 'presence', 10_000);

      const decided = await http(
        'POST',
        `/api/approvals/${approvalId}/decide`,
        { headers: AUTH.reviewer, body: { decision: 'approve' } },
      );
      assert(decided.status === 200, 'E decide ok', decided.body);

      const frame = await waitFrame(
        sock,
        (candidate) =>
          candidate?.type === 'queue' && candidate?.event?.type === 'decided',
        10_000,
      );
      assert(
        frame.event.record.tenantId === undefined,
        'post-restart fan-out remains deployment-scoped',
        frame.event.record,
      );
      sock.close();
    },
  );

  await step(
    'F ticket fail-closed: a valid forge opens, but expired / malformed-run / ' +
      'garbage / cross-channel tickets are refused',
    async () => {
      const spikeRunId = run.runId; // a real spike-owned runId (from A1)

      // POSITIVE CONTROL: a forged-but-VALID hub ticket (correct secret, future
      // exp) OPENS. Proves the shared local secret matches the worker's, so the
      // refusals below are CLAIM rejections, not signature drift. If the secret
      // ever drifts, THIS throws and the probe fails loudly.
      const valid = await forgeTicket({
        channel: 'hub',
        actorId: 'vic',
        role: 'viewer',
        exp: nowSec() + 60,
      });
      const control = connectWs('/api/stream/hub', valid);
      await waitOpen(control, 10_000);
      control.close();

      // Expired hub ticket -> refused (exp in the past).
      const expired = await forgeTicket({
        channel: 'hub',
        actorId: 'vic',
        role: 'viewer',
        exp: nowSec() - 30,
      });
      await expectWsRefused('/api/stream/hub', expired, 8_000);

      // A validly signed RUN ticket with a malformed run id is refused.
      const malformedRun = await forgeTicket({
        channel: 'run',
        runId: 'bad/run',
        actorId: 'mallory',
        role: 'reviewer',
        exp: nowSec() + 60,
      });
      await expectWsRefused(
        `/api/stream/run/demo-approval/${spikeRunId}`,
        malformedRun,
        8_000,
      );

      // Garbage signature -> refused (signature validation).
      await expectWsRefused('/api/stream/hub', 'forged.signature', 8_000);

      // Cross-channel: a hub ticket presented on the run route -> refused.
      const hubForRun = await forgeTicket({
        channel: 'hub',
        actorId: 'vic',
        role: 'viewer',
        exp: nowSec() + 60,
      });
      await expectWsRefused(
        `/api/stream/run/demo-approval/${spikeRunId}`,
        hubForRun,
        8_000,
      );
    },
  );

  // --- Track B background tasks (M-003) -------------------------------------
  await step(
    'G _background rejection (B-S3): a write connector rejects a smuggled ' +
      '_background arg and audits it',
    async () => {
      const { status, body } = await http('POST', '/bg/background-reject');
      assert(status === 200, `bg reject probe -> ${status}`, body);
      assert(body.denied === true, 'the _background arg was rejected', body);
      assert(
        body.policy === 'background',
        "denial policy is 'background'",
        body,
      );
      assert(body.audited === true, 'the denial was audited', body);
    },
  );

  await step(
    'H recovery seam (B-S2): a FRESH init() recovers a task left running in D1',
    async () => {
      // Seed a task the "evicted" instance left 'running' in durable D1.
      const seeded = await http('POST', '/bg/seed-stranded');
      assert(
        seeded.status === 200 && seeded.body.seeded === true,
        'stranded task seeded in D1',
        seeded.body,
      );
      const before = await http('GET', '/bg/task/spike_bs2');
      assert(
        before.body.status === 'running',
        'task is stranded running before recovery',
        before.body,
      );

      // A FRESH BackgroundTaskHost's PUBLIC init() fires recoverStaleTasks (the
      // R-002 pin — no private method). Bodies cannot execute on D1
      // (supportsConcurrentUpdates() === false, R-B1/R-B2), so a maxRetries-0
      // stranded task recovers to 'failed' — the seam firing, on real workerd + D1.
      const recovered = await http('POST', '/bg/recover');
      assert(
        recovered.status === 200 && recovered.body.recovered === true,
        'recover boot ran init()',
        recovered.body,
      );
      const after = await http('GET', '/bg/task/spike_bs2');
      assert(
        after.body.status === 'failed',
        'the stranded task was recovered by the init() seam',
        after.body,
      );
    },
  );

  await step(
    'H2 D1 execution: serialized workflow updates complete and a killed task recovers deployment-scoped',
    async () => {
      const pollTask = async (taskId, wanted, timeoutMs = 30_000) => {
        const deadline = Date.now() + timeoutMs;
        let seen;
        while (Date.now() < deadline) {
          seen = await http(
            'GET',
            `/bg/execution-task/${encodeURIComponent(taskId)}`,
          );
          if (seen.body.status === wanted) return seen.body;
          if (['failed', 'cancelled', 'timed_out'].includes(seen.body.status)) {
            throw new Error(
              `background task terminated: ${JSON.stringify(seen.body)}`,
            );
          }
          await sleep(100);
        }
        throw new Error(
          `background task did not reach ${wanted}: ${JSON.stringify(seen?.body)}`,
        );
      };

      const fast = await http('POST', '/bg/execute');
      assert(fast.status === 200, 'execution task enqueued', fast.body);
      const completed = await pollTask(fast.body.taskId, 'completed');
      assert(
        completed.result?.executed === true,
        'D1 task body executed',
        completed,
      );

      const slow = await http('POST', '/bg/execute-recover');
      assert(slow.status === 200, 'recoverable task enqueued', slow.body);
      await pollTask(slow.body.taskId, 'running');
      await killServer(currentServer);
      await launchServer(
        'bg-recovery',
        stateDir,
        join(tmpDir, 'bg-recovery.log'),
      );
      const recovered = await pollTask(slow.body.taskId, 'completed', 45_000);
      assert(
        recovered.result?.executed === true,
        'killed D1 task recovered and completed',
        recovered,
      );
    },
  );

  // --- Track C signals (M-004) ---------------------------------------------
  // The load-bearing proofs the DL-002 affinity thesis rests on (the DO IS the
  // serialization lease, replacing Redis distributed leasing).
  await step(
    'I affinity (C-S2): a send into an ACTIVE (reserved) loop on the thread ' +
      'DO drains IN-PROCESS via the shared-pubsub registry',
    async () => {
      const { status, body } = await http('POST', '/sig/affinity');
      assert(status === 200, `sig affinity probe -> ${status}`, body);
      assert(
        body.status === 200,
        `thread DO affinity route -> ${body.status}`,
        body,
      );
      // An idle-wake RESERVED a run; the thread now reads ACTIVE in THIS isolate's
      // pubsub-keyed registry. This deterministic probe uses the reserve agent;
      // spike:verify:llm exercises the credentialed real loop separately.
      assert(
        typeof body.activeRunId === 'string',
        'thread reads ACTIVE after the reserve (activeRunId present)',
        body,
      );
      // The second signal resolved to 'deliver' — it JOINED the in-process run
      // rather than being persisted to storage (idle) or discarded. That is the
      // affinity: the send landed in the same isolate hosting the reserved loop.
      assert(
        body.decision?.action === 'deliver',
        "second signal action is 'deliver' (in-process drain, not persist)",
        body.decision,
      );
      assert(
        body.decision?.runId === body.activeRunId,
        'delivered to the SAME reserved run (runId matches)',
        body,
      );
    },
  );

  await step(
    'J malformed thread fail-closed: the topology refuses an invalid DO address',
    async () => {
      const { status, body } = await http('POST', '/sig/malformed-thread');
      assert(status === 200, `sig malformed-thread probe -> ${status}`, body);
      assert(
        body.status === 404,
        'the topology returns 404 before addressing a malformed thread id',
        body,
      );
    },
  );

  // --- Track F goals (M-005) ------------------------------------------------
  // Prove the goal objective surface writes the mastra_thread_state domain and
  // that the DURABLE goal-step read path (resolveGoalStore -> readObjective over
  // the COMPOSED storage) reads it back — including across a workerd restart (the
  // DO-eviction analog). The deterministic gate does not need a model; the
  // credentialed durable-agent loop has its own spike:verify:llm proof.
  const GOAL_OBJECTIVE = 'ship the launch checklist';
  let goalRecord;
  await step(
    'K goal set + read path (F-S1): an objective set via the route lands in D1 ' +
      'and the durable goal-step read path returns it',
    async () => {
      const set = await http('POST', '/goal/set');
      assert(set.status === 200, `goal set probe -> ${set.status}`, set.body);
      assert(
        set.body.status === 200,
        `objective route accepted the set -> ${set.body.status}`,
        set.body,
      );
      assert(
        set.body.record?.objective === GOAL_OBJECTIVE &&
          set.body.record?.status === 'active' &&
          set.body.record?.runsUsed === 0 &&
          set.body.record?.maxRuns === 5,
        'stored record is the fresh active goal (byte shape)',
        set.body.record,
      );
      assert(
        set.body.audited.includes('accepted:'),
        'the accepted write was audited (goal.objective)',
        set.body.audited,
      );
      goalRecord = set.body.record;

      // resolveGoalStore over our composed storage resolves the thread-state
      // domain, and readObjective returns exactly what the route wrote.
      const read = await http('GET', '/goal/read');
      assert(
        read.status === 200,
        `goal read probe -> ${read.status}`,
        read.body,
      );
      assert(
        read.body.storeResolved === true,
        'resolveGoalStore resolved the composed thread-state domain',
        read.body,
      );
      assert(
        read.body.record?.objective === GOAL_OBJECTIVE &&
          read.body.record?.id === goalRecord.id,
        'the goal-step read path returns the record the route wrote',
        read.body.record,
      );
    },
  );

  await step(
    'L goal fail-closed (F-S3): a malformed target is 404 + audited, and an ' +
      'over-cap maxRuns is rejected + audited',
    async () => {
      const malformed = await http('POST', '/goal/malformed-target');
      assert(
        malformed.status === 200,
        `malformed-target probe -> ${malformed.status}`,
        malformed.body,
      );
      assert(
        malformed.body.status === 404,
        'an invalid thread target is 404',
        malformed.body,
      );
      assert(
        malformed.body.audited.includes('rejected:invalid-thread'),
        'the malformed-target write was audited',
        malformed.body.audited,
      );

      const overCap = await http('POST', '/goal/over-cap');
      assert(
        overCap.status === 200,
        `over-cap probe -> ${overCap.status}`,
        overCap.body,
      );
      assert(
        overCap.body.status === 400,
        'an over-cap maxRuns is rejected (never clamped)',
        overCap.body,
      );
      assert(
        overCap.body.audited.includes('rejected:maxruns-over-cap'),
        'the over-cap rejection was audited',
        overCap.body.audited,
      );
    },
  );

  await step(
    'M goal eviction (F-S2): the objective survives a workerd kill+restart and ' +
      'the goal-step read path still returns it (D1, not a registry)',
    async () => {
      await killServer(currentServer);
      await launchServer('gen-4', stateDir, join(tmpDir, 'gen4.log'));
      assert(
        !/address already in use/i.test(currentServer.chunks.join('')),
        'gen-4 log must not contain "address already in use" (orphan trap)',
      );

      const read = await http('GET', '/goal/read');
      assert(
        read.status === 200,
        `post-restart goal read -> ${read.status}`,
        read.body,
      );
      assert(
        read.body.storeResolved === true &&
          read.body.record?.objective === GOAL_OBJECTIVE &&
          read.body.record?.id === goalRecord.id,
        'the DO-evicted goal record is still readable from D1 after restart',
        read.body.record,
      );
    },
  );

  // --- Track D schedules (M-006) --------------------------------------------
  // WE OWN THE TICK (DL-012): listDueSchedules -> CAS claim -> fire, bypassing
  // core's pubsub worker loop. The two load-bearing proofs: one CAS claimant
  // under concurrent ticks, and the stored-context barrier + opaque ID mint on
  // a real DO fire.
  await step(
    'N schedule single claim (D-S1): two CONCURRENT ticks over one due schedule ' +
      'allow one CAS claimant, one trigger row, and one nextFireAt advance',
    async () => {
      const { status, body } = await http('POST', '/sched/concurrent-claim');
      assert(status === 200, `sched concurrent-claim probe -> ${status}`, body);
      // One of the two concurrent ticks won the CAS; the other lost — proving the
      // updateScheduleNextFire compare-and-swap serializes the claim on real D1.
      assert(
        body.fires === 1,
        'one fire across two concurrent tick pollers',
        body,
      );
      assert(body.lost === 1, 'the OTHER tick lost the CAS claim', body);
      assert(body.fireCount === 1, 'the start seam ran for one claimant', body);
      assert(
        body.published === 1,
        'one published trigger row was recorded',
        body,
      );
      assert(
        body.advanced === true,
        'nextFireAt advanced once (the fire is consumed, never hot-looped)',
        body,
      );
    },
  );

  await step(
    'O2 scheduled agent principal (D-S3): an unattended SYSTEM principal reaches ' +
      'the guarded agent through the real Worker->DO hop, and arrives as automation',
    async () => {
      const { status, body } = await http('POST', '/sched/agent');
      assert(status === 200, `sched agent probe -> ${status}`, body);
      // The composition this proves: the system context mints kind:'system', the
      // topology stamps x-flowsafe-principal, the DO rebuilds it as SYSTEM (not
      // as a human), and the agent host admits it because SPIKE_AGENT_META
      // declares system+schedule.fire. Before the principal header was stamped
      // this failed 403 with the whole feature unreachable, and no unit test
      // noticed because they all build the ThreadScope in-process.
      assert(
        body.result?.fired === 1,
        'the agent target fired under an automated principal',
        body,
      );
      assert(body.result?.failed === 0, 'no schedule fire was refused', body);
      assert(
        UUID_PATTERN.test(body.runId),
        'the fired agent runId is an opaque UUID',
        body,
      );
      assert(
        body.runOwner?.kind === 'human' && body.runOwner?.id === 'sched-owner',
        'the fired run inherits the registered schedule owner',
        body,
      );
    },
  );

  await step(
    'O3 automated entry DENIED (D-S4): the same SYSTEM principal on an entry ' +
      'path the agent never declared is refused at the host, through the real hop',
    async () => {
      // The deny direction. Without this the gate is only ever proven to admit,
      // which is how an unreachable gate looked healthy for a whole branch.
      const { status, body } = await http(
        'POST',
        '/sched/agent?entryPath=signal.wake',
      );
      assert(status === 200, `sched agent deny probe -> ${status}`, body);
      assert(
        body.result?.fired === 0 && body.result?.failed === 1,
        'the undeclared entry path did NOT fire',
        body,
      );
      // Generic to the caller on purpose: the entry path and principal kind go
      // to the audit sink, not into a response that a client can probe policy
      // with.
      assert(
        body.error === 'forbidden',
        'the refusal is a generic 403, leaking no policy detail',
        body,
      );
    },
  );

  await step(
    'O schedule barrier + opaque IDs (D-S2): a workflow schedule fires through ' +
      'RunnerRuntime with a fresh UUID, verified context, and initial state',
    async () => {
      const { status, body } = await http('POST', '/sched/barrier');
      assert(status === 200, `sched barrier probe -> ${status}`, body);
      assert(body.fired === 1, 'the workflow target fired', body);
      assert(
        UUID_PATTERN.test(body.runId),
        'the fired runId is an opaque UUID',
        body,
      );
      assert(
        body.status === 'success',
        'the DO ran sched-echo through RunnerRuntime to completion',
        body,
      );
      // The barrier: the FORGED connector id planted in the schedule ROW's stored
      // requestContext never becomes a grant on the executing leg — the verified
      // schedule source strips reserved keys, and the DO's own approvalGrantProvider
      // mints an EMPTY grant list that wins. (The grant KEY
      // is legitimately present as []; the forged VALUE is what must not appear.)
      assert(
        body.leg?.reservedLeaked === false,
        'the forged connector planted in the row did NOT become a grant (barrier holds)',
        body.leg,
      );
      // The DO's OWN workflow scope is present, proving the run went through
      // RunnerRuntime. Flowsafe no longer fabricates Breakwater's isolationScope;
      // keeping the negative assertion makes that boundary a regression tripwire.
      assert(
        body.leg?.workflowScopePresent === true &&
          body.leg?.isolationScopePresent === false,
        'workflow scope is derived and no isolation scope is fabricated',
        body.leg,
      );
      // The stored NON-reserved key is carried from the exact prepared target.
      assert(
        body.leg?.customPresent === true,
        'verified non-reserved stored context reaches the scheduled leg',
        body.leg,
      );
      assert(
        body.leg?.initialStatePresent === true,
        'verified stored initialState reaches core workflow execution',
        body.leg,
      );
    },
  );

  // --- Track E signal providers (M-007) -------------------------------------
  // Webhooks terminate on the Worker, verified BEFORE parse; delivery routes
  // through the topology into the thread inbox; a poll provider survives DO
  // eviction by rehydrating its deployment subscriptions from D1.
  await step(
    'P forged webhook (E-S2): a bad-signature github webhook is rejected BEFORE ' +
      'parse and audited, with no lookup/delivery/state change',
    async () => {
      const forged = await postWebhook(
        'github',
        { repository: { full_name: 'acme/repo' } },
        { forge: true },
      );
      // Rejected at the signature — before any parse, lookup, or delivery.
      assert(
        forged.status === 401,
        `forged webhook -> HTTP ${forged.status} (expected 401)`,
        forged.body,
      );
      // The forgery IS audited (assert content, not just status): the webhook's
      // "auth" IS the signature, so E-S2 requires the forgery recorded.
      const audit = await http('GET', '/sigp/audit');
      assert(audit.status === 200, `audit read -> ${audit.status}`, audit.body);
      const event = audit.body.events.find(
        (e) => e.providerId === 'github' && e.reason === 'forged-signature',
      );
      assert(
        event !== undefined && event.outcome === 'rejected',
        'the forgery was audited as a rejection (forged-signature)',
        audit.body.events,
      );
      // No state change: nothing was delivered (the inbox is still empty here —
      // no subscription has been created yet, and the reject never looked one up).
      const inbox = await http('GET', '/sigp/notifications?threadId=sigp');
      assert(
        inbox.body.count === 0,
        'no notification landed from the forgery (no delivery/state change)',
        inbox.body,
      );
    },
  );

  await step(
    'Q webhook delivery (E-S1): subscribe -> a SIGNED webhook -> notification ' +
      'lands in the thread inbox (mastra_notifications), visible on the read path',
    async () => {
      const sub = await http('POST', '/sigp/subscribe');
      assert(
        sub.status === 200 && sub.body.subscribed === true,
        'subscribed the demo thread to github + the poll provider',
        sub.body,
      );
      // A correctly signed github webhook for the subscribed repo.
      const delivered = await postWebhook('github', {
        action: 'opened',
        repository: { full_name: 'acme/repo' },
        issue: { number: 7 },
      });
      assert(
        delivered.status === 200,
        `signed webhook -> HTTP ${delivered.status}`,
        delivered.body,
      );
      assert(
        delivered.body.matched === 1 && delivered.body.delivered === 1,
        'the webhook matched the subscription ROW and delivered through the topology',
        delivered.body,
      );
      const inbox = await http('GET', '/sigp/notifications?threadId=sigp');
      assert(
        inbox.body.count >= 1 && inbox.body.sources.includes('github'),
        'a github notification landed in mastra_notifications (visible on the read path)',
        inbox.body,
      );
    },
  );

  await step(
    'R poll rehydration (E-S3): after a kill+restart the host DO rehydrates its ' +
      'subscriptions from D1 and fires poll delivery (in-memory-lost, D1-restored)',
    async () => {
      await killServer(currentServer);
      await launchServer('gen-5', stateDir, join(tmpDir, 'gen5.log'));
      assert(
        !/address already in use/i.test(currentServer.chunks.join('')),
        'gen-5 log must not contain "address already in use" (orphan trap)',
      );
      // A FRESH host DO instance: core's in-memory subscription registry is empty,
      // so a correct poll can ONLY come from rehydrating D1 (the E-S3 thesis).
      const poll = await http('POST', '/sigp/poll');
      assert(
        poll.status === 200 && poll.body.status === 200,
        `host DO /poll -> ${poll.body.status}`,
        poll.body,
      );
      assert(
        poll.body.result?.providersPolled === 1,
        'the poll provider was rehydrated from D1 and polled',
        poll.body.result,
      );
      assert(
        poll.body.result?.delivered === 1,
        'the poll fired a delivery (D1-restored subscription -> thread inbox)',
        poll.body.result,
      );
      const inbox = await http('GET', '/sigp/notifications?threadId=sigp');
      assert(
        inbox.body.sources.includes('spike-poller'),
        'the poll notification landed in the thread inbox after the restart',
        inbox.body,
      );
    },
  );

  await step(
    'S deployment sentinel mismatch: a freshly started Worker refuses a D1 ' +
      'provisioned for another deployment',
    async () => {
      await killServer(currentServer);
      await executeLocalD1(
        stateDir,
        "UPDATE flowsafe_deployment SET tenant_tag = 'other' WHERE id = 1;",
      );
      await launchServer('gen-6-mismatch', stateDir, join(tmpDir, 'gen6.log'));
      const refused = await http('GET', '/agents', { headers: AUTH.viewer });
      assert(
        refused.status === 503 &&
          String(refused.body.error ?? '').includes("configured as 'spike'") &&
          String(refused.body.error ?? '').includes("belongs to 'other'"),
        'the Worker fails closed before authentication or routing on sentinel mismatch',
        refused,
      );
    },
  );
}

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => {
    console.error(`\n${signal}: cleaning up`);
    cleanup()
      .catch(() => {})
      .finally(() => process.exit(130));
  });
}

let exitCode = 0;
try {
  await main();
  console.log(
    '\nSPIKE VERIFIED: the authenticated Phase A catalog minted a guarded run, ' +
      'suspended it before the connector, survived process death, rejected ' +
      'evicted stream replay with authoritative status fallback, and restored ' +
      'the original requester when a different reviewer approved. The application ' +
      'input processor and connector each executed exactly once; forged ' +
      'context/headers, disallowed roles, wrong agents/bindings, public raw ' +
      'resume, and stale decision replay all ' +
      'failed closed. The lower-level workflow resumed via its exact-leg approval ' +
      'grant, a forged raw resume failed closed, and self-decision was denied. ' +
      'Live streaming (M-009): a decided event fanned out through the singleton ' +
      'deployment hub to multiple actors, survived a kill+restart, and refused ' +
      'expired / malformed-run / garbage / cross-channel tickets. ' +
      'Track B (M-003): a smuggled _background arg was rejected + audited ' +
      '(B-S3), and a fresh init() recovered a task left running in D1 (B-S2). ' +
      'The serialized deployment-scoped D1 domains executed a task to completion ' +
      'and recovered a killed in-flight task after restart (H2). ' +
      'Track C (M-004): a send into an ACTIVE thread-DO loop drained IN-PROCESS ' +
      'via the shared-pubsub registry (C-S2, the DL-002 affinity thesis), and a ' +
      'malformed thread address failed closed at the topology. Track F (M-005): ' +
      'an objective set via the route landed ' +
      'in mastra_thread_state and the durable goal-step read path returned it ' +
      '(F-S1), survived a kill+restart (F-S2), and a malformed target + over-cap ' +
      'maxRuns failed closed and audited (F-S3). Track D (M-006): two concurrent ' +
      'ticks over one due schedule allowed one CAS claimant (D-S1), and a ' +
      'workflow schedule fired through RunnerRuntime with a fresh opaque runId ' +
      'while a reserved key stayed absent and verified initial state arrived (D-S2). ' +
      'Track E (M-007): a forged webhook was rejected BEFORE parse and audited ' +
      '(E-S2), a signed webhook matched its subscription row and landed a ' +
      'notification in the thread inbox (E-S1), a poll provider rehydrated its ' +
      'subscriptions from D1 after a kill+restart and fired delivery (E-S3). ' +
      'Finally, a fresh Worker refused a D1 sentinel stamped for another ' +
      'deployment with 503 before authentication or routing.',
  );
} catch (error) {
  exitCode = 1;
  console.error(`\nFAILED at [${currentStep}]: ${error.message}`);
  for (const server of servers) {
    dumpLog(server);
  }
} finally {
  try {
    await cleanup();
  } catch (error) {
    exitCode = 1;
    console.error(`cleanup failed: ${error.message}`);
  }
  const state = await portState();
  if (state !== 'refused') {
    exitCode = 1;
    console.error(`post-run leak: port ${PORT} is ${state}, expected refused`);
  }
}
process.exitCode = exitCode;
