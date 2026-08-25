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
// task, signal, goal, schedule, and webhook compatibility proofs, plus the
// per-suspension deadline scenario: a run arms its own Durable Object alarm
// inside its Mastra suspend payload, the process is killed, and the restarted
// object resumes the run ITSELF with the reserved timeout envelope.
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
  EXECUTION_FENCE_DDL,
  EXECUTION_FENCE_ROW_ID,
  EXECUTION_FENCE_TABLE,
} from '#deployment-identity-protocol';
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
// Suspension deadlines (T1-T3): the run's own DO alarm resumes a suspension
// whose awaited signal never arrives. `deadlineMs` rides the run input so ONE
// spike workflow drives both sides of the fence. The TIMEOUT deadline has to
// outlive every step between arming and the kill — a wake that fired while the
// process was still alive would prove nothing about surviving process death —
// and still elapse inside the kill+restart window, so it adds no wall clock of
// its own; hence 10s rather than the module's 1s floor. The SIGNAL deadline
// only has to outlive its own immediate resume, which settles the entry.
// Idempotent start (FI1/FI2): demo-approval's shape with a counting first step
// (spike/worker.ts COUNTED_WORKFLOW_ID). MUST match the worker's id.
const COUNTED_WORKFLOW_ID = 'demo-idempotent';
const DEADLINE_WORKFLOW_ID = 'demo-deadline';
const DEADLINE_STEP = 'wait-signal';
// The reserved arming key, as it appears on the wire (do-runner exports it to
// TypeScript authors as SUSPENSION_DEADLINE_PAYLOAD_KEY).
const DEADLINE_PAYLOAD_KEY = 'flowsafe.deadlineMs';
const TIMEOUT_DEADLINE_MS = 10_000;
const SIGNAL_DEADLINE_MS = 30_000;
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

// --- Suspension deadline (T1-T3) helpers -----------------------------------

// Read the wake state the run's OWN Durable Object holds: the persisted fenced
// entry plus the single alarm its two duties share (spike/worker.ts
// handleSuspensionDeadlineProbe). Nothing on the run surface exposes either.
async function armedDeadline(runId) {
  const { status, body } = await http(
    'GET',
    `/deadline/armed?workflowId=${DEADLINE_WORKFLOW_ID}&runId=${encodeURIComponent(runId)}`,
  );
  assert(status === 200, `armed-deadline probe -> ${status}`, body);
  assert(
    body.status === 200,
    `run DO armed-deadline route -> ${body.status}`,
    body,
  );
  return body.armed;
}

// A wake settles its consumed entry AFTER the resume it just ran, so a status
// that already reads 'success' can precede that write by a few milliseconds.
// Poll for the settled state instead of racing the tail of the wake, and return
// the last observation either way so a real failure still reports what it saw.
async function settledDeadline(runId, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let armed;
  while (Date.now() < deadline) {
    armed = await armedDeadline(runId);
    if (armed.record === null && armed.alarmAt === null) return armed;
    await sleep(250);
  }
  return armed;
}

function deadlineRunPath(runId) {
  return `/runs/${DEADLINE_WORKFLOW_ID}/${encodeURIComponent(runId)}`;
}

// --- Execution fence (F1) helpers ------------------------------------------
// The fence control channel and the two assertions every fence probe makes:
// the exact refusal CODE (not merely "some 5xx"), and the state it names.

const FENCE_ADMIN_PATH = '/admin/execution-fence';

async function readFence() {
  const { status, body } = await http('GET', FENCE_ADMIN_PATH);
  assert(status === 200, `GET ${FENCE_ADMIN_PATH} -> ${status}`, body);
  return body;
}

async function moveFence(expected, next, proofKey) {
  const { status, body } = await http('POST', FENCE_ADMIN_PATH, {
    body: { expected, next, ...(proofKey === undefined ? {} : { proofKey }) },
  });
  assert(
    status === 200 && body.state === next,
    `fence CAS ${expected} -> ${next} failed`,
    { status, body },
  );
  return body;
}

// A keyed start, as a trusted client actually sends one: the key rides the
// PUBLIC body (it names a request, unlike the runId, which stays server-minted
// and is still refused with a 400 here).
async function startWithKey(idempotencyKey, headers = AUTH.operator) {
  return http('POST', '/runs', {
    body: { ...RUN_BODY, idempotencyKey },
    headers,
  });
}

// The same keyed start against the COUNTING workflow, whose first step
// increments a durable D1 row before the run suspends. `counterId` names the
// row, so each probe counts only its own executions.
async function startCountedWithKey(
  idempotencyKey,
  counterId,
  headers = AUTH.operator,
) {
  return http('POST', '/runs', {
    body: {
      workflowId: COUNTED_WORKFLOW_ID,
      inputData: { topic: 'launch', counterId },
      idempotencyKey,
    },
    headers,
  });
}

// How many times the counting step actually ran for this counter. THE
// assertion FI1 and FI2 are really about: a repeated run id says two responses
// named one run, while this says the paid work happened once.
async function executionCount(counterId) {
  const { status, body } = await http(
    'GET',
    `/idempotent/executions?counterId=${encodeURIComponent(counterId)}`,
  );
  assert(status === 200, `execution-count probe -> ${status}`, body);
  return body.executions;
}

// Every fence refusal is 503 (operator-transient, retryable) carrying
// reason.code EXECUTION_FENCED and the state that refused. Asserting the code
// and the state — rather than the status alone — is what separates "the fence
// refused" from "something else broke with a 5xx".
function assertFenced(label, response, state) {
  assert(
    response.status === 503 &&
      response.body?.reason?.code === 'EXECUTION_FENCED' &&
      response.body?.reason?.state === state,
    `${label} must be refused 503 EXECUTION_FENCED in '${state}'`,
    response,
  );
}

// --- Drain inventory (F2) helpers ------------------------------------------

const INVENTORY_ADMIN_PATH = '/admin/inventory';

async function readInventoryIndex() {
  const { status, body } = await http('GET', INVENTORY_ADMIN_PATH);
  assert(status === 200, `GET ${INVENTORY_ADMIN_PATH} -> ${status}`, body);
  return body;
}

// One category, PAGED TO EXHAUSTION through the inventory's own cursors — the
// sweep an operator's drain proof is actually made of, not a single page that
// could hide the rows behind it. `limit: 3` forces several continuations even
// on the spike's small data, so the keyset is exercised rather than skipped.
async function sweepInventoryCategory(category) {
  const keys = [];
  let first;
  let cursor;
  for (let pass = 0; pass < 200; pass += 1) {
    const query = `${INVENTORY_ADMIN_PATH}?category=${encodeURIComponent(category)}&limit=3${
      cursor === undefined ? '' : `&cursor=${encodeURIComponent(cursor)}`
    }`;
    const { status, body } = await http('GET', query);
    assert(status === 200, `GET ${query} -> ${status}`, body);
    first ??= body;
    for (const entry of body.entries) keys.push(entry.key.join('/'));
    if (body.cursor === undefined) {
      return {
        keys,
        count: first.count,
        totals: first.totals,
        class: body.class,
        table: body.table,
      };
    }
    cursor = body.cursor;
  }
  throw new Error(`inventory paging did not terminate for ${category}`);
}

// Every WORK category, swept. The drain proof is defined over exactly these.
async function sweepInventoryWork(index) {
  const work = index.categories.filter((entry) => entry.class === 'work');
  const swept = {};
  for (const entry of work) {
    swept[entry.category] = await sweepInventoryCategory(entry.category);
  }
  return swept;
}

async function startFencedRun(headers = AUTH.operator) {
  const { status, body } = await http('POST', '/runs', {
    body: RUN_BODY,
    headers,
  });
  assert(
    status === 200 && body.status === 'suspended',
    `fence-probe run did not suspend -> ${status}`,
    body,
  );
  assert(
    typeof body.approval?.id === 'string',
    'fence-probe approval id',
    body,
  );
  return {
    runId: body.runId,
    approvalId: body.approval.id,
    step: body.suspended?.[0],
  };
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

  // Provisioning, as the protocol performs it: the ownership sentinel AND an
  // EXPLICIT execution fence row. The fence DDL is the protocol's own constant
  // (the same string do-runner/execution-fence.ts issues), so this database is
  // shaped exactly like one flowsafe-provision would have produced — a
  // hand-copied schema here would let the store's CREATE TABLE IF NOT EXISTS
  // silently accept a table with no CHECK constraints. 'open' because every
  // scenario before the fence probes needs an executing deployment.
  await step('provision deployment identity sentinel and execution fence', () =>
    executeLocalD1(
      stateDir,
      'CREATE TABLE IF NOT EXISTS flowsafe_deployment (id INTEGER PRIMARY KEY CHECK (id = 1), tenant_tag TEXT NOT NULL, provisioned_at TEXT NOT NULL); ' +
        "INSERT OR IGNORE INTO flowsafe_deployment (id, tenant_tag, provisioned_at) VALUES (1, 'spike', datetime('now')); " +
        `${EXECUTION_FENCE_DDL}; ` +
        `INSERT OR IGNORE INTO ${EXECUTION_FENCE_TABLE} (id, state, proof_key, proof_run_id, updated_at) ` +
        `VALUES ('${EXECUTION_FENCE_ROW_ID}', 'open', NULL, NULL, ${Date.now()});`,
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
    'I2 content policy (C-S6): the thread DO refuses denied model-visible ' +
      'text and passes clean text through the same gate',
    async () => {
      const { status, body } = await http('POST', '/sig/content-policy');
      assert(status === 200, `sig content-policy probe -> ${status}`, body);
      // Denied: a 422 whose body names neither the policy nor the content.
      assert(
        body.denied?.status === 422,
        'denied signal content -> 422 at the thread DO',
        body.denied,
      );
      assert(
        body.denied?.body === '{"error":"signal content denied"}',
        'the refusal is the static, opaque denial body',
        body.denied,
      );
      assert(
        !String(body.denied?.body ?? '').includes('spike-denied-content'),
        'the refusal does not echo the inspected content',
        body.denied,
      );
      // Allowed: the same gate leaves an ordinary signal alone.
      assert(
        body.allowed?.status === 200,
        'clean signal content still delivers through the same routes',
        body.allowed,
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
        body.result?.due === 1,
        "only the probe's own schedule was due in its window (isolation holds)",
        body,
      );
      assert(
        Array.isArray(body.triggers) && body.triggers.length === 1,
        "exactly one trigger row exists for the probe's own schedule",
        body,
      );
      assert(
        body.triggers?.[0]?.runId === body.runId,
        'the persisted trigger row names the run the seam dispatched',
        body,
      );
      assert(
        body.triggers?.[0]?.outcome === 'succeeded',
        "the probe's fire settled as succeeded (threaded agent lane receipt)",
        body,
      );
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

  // --- Suspension deadlines (T1-T3): the run resumes ITSELF -----------------
  // The one mechanism the unit tests can only assert against a hand-written
  // DurableObjectState stub: a step suspends with a deadline inside its own
  // Mastra suspend payload, the run's OWN Durable Object persists a fenced
  // entry and arms its single alarm for it, the process DIES, and the restarted
  // object wakes itself and resumes the run with the reserved timeout envelope
  // under the system principal — with no client ever calling resume.
  const signalRun = await step(
    'T1 signal fence: a real signal before the deadline resumes the run as its ' +
      'human requester and settles the armed entry',
    async () => {
      const started = await http('POST', '/runs', {
        headers: AUTH.operator,
        body: {
          workflowId: DEADLINE_WORKFLOW_ID,
          inputData: { topic: 'launch', deadlineMs: SIGNAL_DEADLINE_MS },
        },
      });
      assert(
        started.status === 200 && started.body.status === 'suspended',
        'signal-fence run suspended at its timed step',
        { status: started.status, body: started.body },
      );
      assert(
        JSON.stringify(started.body.suspended?.[0]) ===
          JSON.stringify([DEADLINE_STEP]),
        `suspended[0] is ['${DEADLINE_STEP}']`,
        started.body.suspended,
      );
      const resumed = await http(
        'POST',
        `${deadlineRunPath(started.body.runId)}/resume`,
        {
          headers: AUTH.operator,
          body: {
            step: started.body.suspended[0],
            resumeData: { signal: 'launch' },
          },
        },
      );
      assert(
        resumed.status === 200 && resumed.body.status === 'success',
        'the signalled run resumed to success',
        resumed.body,
      );
      // The step branched on the exported guard, so this is the negative half
      // of the SAME contract T3 proves: a real signal must not look like a
      // timeout to the step that receives it.
      assert(
        resumed.body.result?.resumedBy === 'signal' &&
          resumed.body.result?.timeoutStep === undefined,
        'the step saw a real signal, not a timeout envelope',
        resumed.body.result,
      );
      assert(
        resumed.body.requestedBy === 'opal' &&
          resumed.body.requestedByKind === 'human',
        'provenance names the human whose signal advanced the run',
        resumed.body,
      );
      // The fence path: the resume settles the armed entry, so there is nothing
      // left for a later wake to resume a run that has already moved on.
      const armed = await armedDeadline(started.body.runId);
      assert(
        armed.record === null && armed.alarmAt === null,
        'the signalled run keeps no armed deadline and no alarm',
        armed,
      );
      return { runId: started.body.runId };
    },
  );

  const deadlineRun = await step(
    'T2 deadline arm: a suspension carrying the reserved deadline key arms the ' +
      "run's own DO alarm with a fenced entry",
    async () => {
      const started = await http('POST', '/runs', {
        headers: AUTH.operator,
        body: {
          workflowId: DEADLINE_WORKFLOW_ID,
          inputData: { topic: 'launch', deadlineMs: TIMEOUT_DEADLINE_MS },
        },
      });
      assert(
        started.status === 200 && started.body.status === 'suspended',
        'timed run suspended at its timed step',
        { status: started.status, body: started.body },
      );
      // The arming value travels inside MASTRA's suspend payload (the step
      // declares no suspendSchema, so nothing strips the reserved key), and the
      // DO derives its record from this authoritative summary.
      assert(
        started.body.suspendPayload?.[DEADLINE_STEP]?.[DEADLINE_PAYLOAD_KEY] ===
          TIMEOUT_DEADLINE_MS,
        'the reserved deadline key survived into the authoritative summary',
        started.body.suspendPayload,
      );
      const suspendedAt = started.body.suspendedAt?.[DEADLINE_STEP];
      assert(
        Number.isSafeInteger(suspendedAt),
        'the summary carries the suspendedAt fence the entry is armed against',
        started.body.suspendedAt,
      );

      const armed = await armedDeadline(started.body.runId);
      const entry = armed.record?.entries?.[0];
      assert(
        armed.record?.workflowId === DEADLINE_WORKFLOW_ID &&
          armed.record?.runId === started.body.runId &&
          armed.record?.entries?.length === 1 &&
          entry?.step === DEADLINE_STEP &&
          entry?.deadlineAt === suspendedAt + TIMEOUT_DEADLINE_MS &&
          entry?.suspendedAt === suspendedAt &&
          entry?.resumeCount === 0 &&
          entry?.attempts === undefined,
        'the run DO persisted ONE entry, fenced to this suspension and due at ' +
          'suspendedAt + deadlineMs (never now + deadlineMs)',
        armed,
      );
      // ONE alarm serves both DO duties, so this also proves the suspension due
      // time WON the min() — it is not the 60s run-owner recovery wake.
      assert(
        typeof armed.alarmAt === 'number' &&
          armed.alarmAt <= entry.deadlineAt + 5_000,
        'the object armed its single alarm for the suspension deadline',
        armed,
      );
      return { runId: started.body.runId, deadlineAt: entry.deadlineAt };
    },
  );

  await step(
    'T3 deadline wake: the armed alarm survives a workerd kill+restart and the ' +
      'run resumes ITSELF with the timeout envelope under the system principal',
    async () => {
      // Still suspended on THIS process, so the wake has not fired yet:
      // whatever resumes this run has to come from the restarted object.
      const before = await http('GET', deadlineRunPath(deadlineRun.runId), {
        headers: AUTH.viewer,
      });
      assert(
        before.status === 200 && before.body.status === 'suspended',
        'the timed run is still suspended when its process is killed',
        before.body,
      );

      // Captured BEFORE the kill and asserted against the envelope below: the
      // pre-kill read above proves the run had not resumed YET, but the wake
      // could still fire in the gap between that read and the kill. Only an
      // expiry stamped at or after the kill proves the RESTARTED process's
      // timer did the work rather than the original one's.
      const killedAt = Date.now();
      await killServer(currentServer);
      // The deadline elapses with NOTHING running: no isolate, no timer, no
      // client. Only the persisted entry and the object's own alarm survive.
      await launchServer(
        'deadline-alarm',
        stateDir,
        join(tmpDir, 'deadline-alarm.log'),
      );
      assert(
        !/address already in use/i.test(currentServer.chunks.join('')),
        'deadline-alarm log must not contain "address already in use" (orphan trap)',
      );

      const pollDeadlineRun = async (timeoutMs) => {
        const deadline = Date.now() + timeoutMs;
        let seen;
        while (Date.now() < deadline) {
          seen = await http('GET', deadlineRunPath(deadlineRun.runId), {
            headers: AUTH.viewer,
          });
          if (seen.body.status === 'success') return seen;
          // 'running' is the resume the wake is executing right now; only a
          // terminal status other than success means the wake went wrong.
          if (['failed', 'cancelled', 'timed_out'].includes(seen.body.status)) {
            throw new Error(
              `timed run terminated without succeeding: ${JSON.stringify(seen.body)}`,
            );
          }
          await sleep(250);
        }
        throw new Error(
          `the suspension deadline never resumed the run: ${JSON.stringify(seen?.body)}`,
        );
      };
      const resumed = await pollDeadlineRun(60_000);
      assert(
        resumed.body.result?.resumedBy === 'timeout' &&
          resumed.body.result?.timeoutStep === DEADLINE_STEP &&
          resumed.body.result?.deadlineAt === deadlineRun.deadlineAt &&
          resumed.body.result?.expiredAt >= deadlineRun.deadlineAt &&
          resumed.body.result?.expiredAt >= killedAt &&
          deadlineRun.deadlineAt > killedAt &&
          resumed.body.requestedBy === 'flowsafe-suspension-deadline' &&
          resumed.body.requestedByKind === 'system',
        'the alarm armed before the kill fired on the RESTARTED process and ' +
          'resumed the run itself: the step saw the reserved timeout envelope ' +
          '(isSuspensionTimeoutResumeData) for its own step and deadline, under ' +
          'the system principal, with no client calling resume, the deadline ' +
          'was still in the future when the process died, and the wake acted ' +
          'after the kill rather than in the gap before it',
        { killedAt, body: resumed.body },
      );
      // The consumed entry is settled, so the wake cannot repeat: one resume
      // per expired deadline, and no alarm left behind for a finished run.
      const armed = await settledDeadline(deadlineRun.runId, 10_000);
      assert(
        armed.record === null && armed.alarmAt === null,
        'the consumed entry was settled and the shared alarm cleared',
        armed,
      );
      // The T1 run is untouched across the same restart: its entry was settled
      // by the real signal, so no timeout resume can reach it — the run still
      // reads as the human signal advanced it, and its step ran once.
      const signalled = await http('GET', deadlineRunPath(signalRun.runId), {
        headers: AUTH.viewer,
      });
      assert(
        signalled.status === 200 &&
          signalled.body.status === 'success' &&
          signalled.body.result?.resumedBy === 'signal' &&
          signalled.body.requestedBy === 'opal' &&
          signalled.body.requestedByKind === 'human',
        'the signalled run kept its human resume across the restart (the fence ' +
          'held: no timeout resume reached it)',
        signalled.body,
      );
    },
  );

  // --- Execution fence (F1): the migration control, on real workerd ---------
  // Unit tests can prove the store's compare-and-set. What they cannot prove is
  // that a fence written by one process still refuses work in the NEXT one, and
  // that the refusal reaches an HTTP client as the taxonomy's own code rather
  // than a generic 500 somewhere in the router chain. That is this scenario.

  const fenceRuns = await step(
    'FE0 fence baseline: the provisioned deployment is open and minting work',
    async () => {
      const initial = await readFence();
      assert(
        initial.state === 'open' &&
          initial.proofKey === undefined &&
          initial.proofRunId === undefined,
        'provisioning seeded an explicit open fence with no proof binding',
        initial,
      );
      // Two suspended runs minted while OPEN. One is drained through the
      // fence's draining state, the other is left for the lock to refuse —
      // both have to exist before the first transition, because a fenced
      // deployment mints nothing.
      const draining = await startFencedRun();
      const locked = await startFencedRun();
      assert(
        draining.runId !== locked.runId,
        'fence probes need two distinct runs',
        { draining, locked },
      );
      return { draining, locked };
    },
  );

  await step(
    'FE1 draining: new starts are refused EXECUTION_FENCED while an existing ' +
      "run's approval still resumes it to completion",
    async () => {
      await moveFence('open', 'draining');

      const refusedStart = await http('POST', '/runs', {
        body: RUN_BODY,
        headers: AUTH.operator,
      });
      assertFenced('a run start under draining', refusedStart, 'draining');

      // The whole point of draining: outstanding work must still be able to
      // finish, or the drain can never complete. A different actor decides
      // (separation of duties), and the resume runs the workflow to success.
      const decided = await http(
        'POST',
        `/api/approvals/${fenceRuns.draining.approvalId}/decide`,
        { headers: AUTH.reviewer, body: { decision: 'approve' } },
      );
      assert(
        decided.status === 200 &&
          decided.body.resume?.summary?.status === 'success',
        'a draining deployment must still resume an already-suspended run',
        decided,
      );
    },
  );

  await step(
    'FE2 migration-locked: resume, approval decide, and start are all refused, ' +
      'and the refused decision is not committed',
    async () => {
      await moveFence('draining', 'migration-locked');

      const refusedStart = await http('POST', '/runs', {
        body: RUN_BODY,
        headers: AUTH.operator,
      });
      assertFenced(
        'a run start under migration-locked',
        refusedStart,
        'migration-locked',
      );

      const refusedDecide = await http(
        'POST',
        `/api/approvals/${fenceRuns.locked.approvalId}/decide`,
        { headers: AUTH.reviewer, body: { decision: 'approve' } },
      );
      assertFenced(
        'an approval decision under migration-locked',
        refusedDecide,
        'migration-locked',
      );

      // The gate sits BEFORE the decision's compare-and-set: a decision that
      // committed here would be durably recorded on a deployment that can never
      // act on it, and the deployment taking over would inherit a decided
      // approval with no resume behind it.
      const stillPending = await http(
        'GET',
        `/api/approvals/${fenceRuns.locked.approvalId}`,
        { headers: AUTH.viewer },
      );
      assert(
        stillPending.status === 200 &&
          stillPending.body.status === 'pending' &&
          stillPending.body.decidedBy === undefined,
        'the refused decision left the approval untouched',
        stillPending,
      );

      // The run DO's own resume gate, reached directly rather than through the
      // approval service — the fence has to hold on both paths.
      const refusedResume = await http(
        'POST',
        `/runs/${RUN_BODY.workflowId}/${encodeURIComponent(fenceRuns.locked.runId)}/resume`,
        {
          headers: AUTH.operator,
          body: {
            step: fenceRuns.locked.step,
            resumeData: { approved: true },
          },
        },
      );
      assertFenced(
        'a raw resume under migration-locked',
        refusedResume,
        'migration-locked',
      );
    },
  );

  await step(
    'FE3 fence persistence: the lock survives a workerd kill+restart and the ' +
      'restarted process still refuses to mint work',
    async () => {
      await killServer(currentServer);
      await launchServer(
        'fence-restart',
        stateDir,
        join(tmpDir, 'fence-restart.log'),
      );
      assert(
        !/address already in use/i.test(currentServer.chunks.join('')),
        'fence-restart log must not contain "address already in use" (orphan trap)',
      );

      // Nothing in the new process has ever seen a fence transition: the only
      // thing that carried the lock across process death is the D1 row.
      const restored = await readFence();
      assert(
        restored.state === 'migration-locked',
        'the fence state survived process death',
        restored,
      );
      const refusedStart = await http('POST', '/runs', {
        body: RUN_BODY,
        headers: AUTH.operator,
      });
      assertFenced(
        'a run start on the restarted locked deployment',
        refusedStart,
        'migration-locked',
      );
    },
  );

  await step(
    'FE4 reopen: migration-locked -> open restores minting, and the approval ' +
      'the lock refused now completes its run',
    async () => {
      await moveFence('migration-locked', 'open');
      assert(
        (await readFence()).state === 'open',
        'the reopened fence reads back as open',
      );

      const started = await startFencedRun();
      assert(
        started.runId !== fenceRuns.locked.runId,
        'the reopened deployment minted a fresh run',
        started,
      );

      // Nothing was lost under the lock: the approval that was refused is still
      // pending and still resumes its run.
      const decided = await http(
        'POST',
        `/api/approvals/${fenceRuns.locked.approvalId}/decide`,
        { headers: AUTH.reviewer, body: { decision: 'approve' } },
      );
      assert(
        decided.status === 200 &&
          decided.body.resume?.summary?.status === 'success',
        'the approval refused under the lock completes once the fence reopens',
        decided,
      );
    },
  );

  // --- Idempotent start (F3): exactly-once, on real workerd -----------------
  // Unit tests can prove the reservation's compare-and-set over node:sqlite.
  // What they cannot prove is that a reservation written by one PROCESS still
  // converges a retry in the next one, that two genuinely concurrent requests
  // reach one run through real D1 rather than a synchronous test double, and
  // that the fence's proof-only state admits exactly the start carrying its
  // nominated key once the whole router chain is in the way.

  const idempotentRun = await step(
    'FI1 idempotent start: a retry after a workerd kill+restart returns the ' +
      'SAME run, and the paid first step ran exactly ONCE',
    async () => {
      const first = await startCountedWithKey('spike-key-1', 'fi1');
      assert(
        first.status === 200 && first.body.status === 'suspended',
        'the first keyed start must run normally',
        first,
      );
      // The start responded with the SUSPENDED summary, so the counting step
      // has already run and its row is durable. Anything other than 1 here
      // would mean the probe itself is not measuring what it claims to.
      assert(
        (await executionCount('fi1')) === 1,
        'the first keyed start must execute the counting step exactly once',
      );

      // Process death between the response and the retry — the case a client
      // cannot tell from a lost response.
      await killServer(currentServer);
      await launchServer(
        'idempotent-restart',
        stateDir,
        join(tmpDir, 'idempotent-restart.log'),
      );

      const retry = await startCountedWithKey('spike-key-1', 'fi1');
      assert(
        retry.status === 200 && retry.body.runId === first.body.runId,
        'the retry must replay the first run, not start a second',
        { first: first.body, retry: retry.body },
      );
      // The claim this probe exists for, stated as EXECUTIONS rather than as
      // run ids: the retry answered from the reservation and ran nothing.
      // Nothing in the restarted process had ever seen this key — the only
      // thing that carried it across process death is the D1 row.
      const executions = await executionCount('fi1');
      assert(
        executions === 1,
        'a retry after process death must execute the first step no second time',
        { executions, first: first.body, retry: retry.body },
      );
      return { runId: first.body.runId, approvalId: first.body.approval?.id };
    },
  );

  await step(
    'FI2 concurrent same-key starts: two in-flight requests produce ONE run ' +
      'and ONE execution',
    async () => {
      // The cross-isolate race a kill-and-retry harness cannot fake: neither
      // request has seen the other, and only the reservation's CAS is between
      // them. On the agent surface this is the ONLY thing between them, because
      // two same-key starts naming different threads are two different Durable
      // Objects with no shared lock at all.
      const [a, b] = await Promise.all([
        startCountedWithKey('spike-key-2', 'fi2'),
        startCountedWithKey('spike-key-2', 'fi2'),
      ]);
      const responses = [a, b];
      const accepted = responses.filter((response) => response.status === 200);
      const runIds = new Set(accepted.map((response) => response.body.runId));
      // At least one caller must be ANSWERED, not merely refused consistently:
      // a burst in which both requests were told to retry would satisfy every
      // "no second run" assertion below while proving nothing about the start.
      assert(
        accepted.length >= 1,
        'at least one concurrent same-key start must be answered with a run',
        {
          a: { status: a.status, body: a.body },
          b: { status: b.status, body: b.body },
        },
      );
      assert(
        runIds.size === 1,
        'two concurrent same-key starts must resolve to exactly one run',
        {
          a: { status: a.status, body: a.body },
          b: { status: b.status, body: b.body },
        },
      );
      // The loser is allowed to replay (200), to be told the winner is still
      // working (503 IDEMPOTENT_START_PENDING), or — inside the window between
      // the winning claim and its dispatch — to be told UNRESOLVABLE (409).
      // What it may never be is a second run, and what it may never carry is a
      // code outside the published taxonomy: a collapsed or generic refusal
      // would read to a client as "retry", which is exactly the advice that
      // charges twice.
      const refusalCodes = new Map();
      for (const response of responses) {
        if (response.status === 200) continue;
        const code = response.body?.reason?.code;
        assert(
          code === 'IDEMPOTENT_START_PENDING' ||
            code === 'IDEMPOTENT_START_UNRESOLVABLE',
          'a concurrent same-key start was refused outside the taxonomy',
          { status: response.status, body: response.body },
        );
        refusalCodes.set(code, (refusalCodes.get(code) ?? 0) + 1);
      }
      // One 200 carries the suspended summary, so the counting step has
      // already run by the time both requests have settled — no polling, and
      // no window in which a second execution could still be in flight.
      const executions = await executionCount('fi2');
      assert(
        executions === 1,
        'a concurrent same-key burst must execute the first step exactly once',
        {
          executions,
          accepted: accepted.length,
          refusals: Object.fromEntries(refusalCodes),
        },
      );
      console.log(
        `  answered: ${accepted.length}, refusals: ${JSON.stringify(Object.fromEntries(refusalCodes))}`,
      );
    },
  );

  await step(
    'FI3 proof-only: the nominated key is admitted, every other start is ' +
      'refused, and the proof binds to exactly one run',
    async () => {
      await moveFence('open', 'proof-only', 'spike-proof-key');
      const nominated = await readFence();
      assert(
        nominated.state === 'proof-only' &&
          nominated.proofKey === 'spike-proof-key' &&
          nominated.proofRunId === undefined,
        'entering proof-only nominates a key and binds no run yet',
        nominated,
      );

      // A start with no key at all: refused, exactly as under the lock.
      const unkeyed = await http('POST', '/runs', {
        body: RUN_BODY,
        headers: AUTH.operator,
      });
      assertFenced('an unkeyed start under proof-only', unkeyed, 'proof-only');

      // A start carrying a DIFFERENT key: also refused. The key is not a
      // password, it is a nomination — only the operator's own key matches.
      const wrongKey = await startWithKey('spike-wrong-key');
      assertFenced(
        'a start carrying the wrong key under proof-only',
        wrongKey,
        'proof-only',
      );

      // The nominated start runs, and binds the proof.
      const proof = await startWithKey('spike-proof-key');
      assert(
        proof.status === 200 && proof.body.status === 'suspended',
        'the nominated proof start must be admitted',
        proof,
      );
      const bound = await readFence();
      assert(
        bound.proofRunId === proof.body.runId,
        'the admitted proof start binds the fence to its run',
        { bound, proof: proof.body },
      );

      // Reopening restores ordinary minting, and clears the proof binding so
      // the next proof cannot inherit this one's run.
      await moveFence('proof-only', 'open');
      const reopened = await readFence();
      assert(
        reopened.state === 'open' &&
          reopened.proofKey === undefined &&
          reopened.proofRunId === undefined,
        'reopening clears the proof nomination and its binding',
        reopened,
      );
      const afterReopen = await startWithKey('spike-key-3');
      assert(
        afterReopen.status === 200 &&
          afterReopen.body.runId !== proof.body.runId &&
          afterReopen.body.runId !== idempotentRun.runId,
        'the reopened deployment mints fresh runs again',
        afterReopen,
      );
    },
  );

  // --- Drain inventory (F2): the migration proof, on real workerd -----------
  // Unit tests can prove the queries are pure SELECTs against the real schemas.
  // What they cannot prove is that those queries read the SAME D1 that real
  // HTTP traffic on real workerd wrote its runs, approvals, reservations,
  // notifications, and tasks into — and that the numbers MOVE as that work
  // drains. An inventory that reported the right shape over the wrong database
  // would pass every unit test and certify a deployment that still owed work.

  await step(
    'FV1 inventory index: every category, its class, the states no query can ' +
      'see, and the rule an empty answer means something under',
    async () => {
      const index = await readInventoryIndex();
      const work = index.categories
        .filter((entry) => entry.class === 'work')
        .map((entry) => entry.category);
      const standing = index.categories
        .filter((entry) => entry.class === 'standing')
        .map((entry) => entry.category);
      assert(
        work.length === 7 && standing.length === 2,
        'the index splits work from standing configuration',
        { work, standing },
      );
      assert(
        index.unenumerable.some(
          (entry) => entry.name === 'run-owner-recovery-journal',
        ),
        'the Durable Object journal window is DECLARED, not silently omitted',
        index.unenumerable,
      );
      assert(
        Array.isArray(index.drainProof?.reachableFrom) &&
          index.drainProof.reachableFrom.length === 1 &&
          index.drainProof.reachableFrom[0] === 'draining',
        "an empty work set only means something from 'draining'",
        index.drainProof,
      );
    },
  );

  await step(
    'FV2 drain proof: seeded work appears while draining, leaves as it is ' +
      'finished, and reads empty across TWO consecutive sweeps before the lock',
    async () => {
      const index = await readInventoryIndex();

      // A BASELINE, because this deployment has been running scenarios for the
      // whole spike and legitimately still holds work from them. The proof
      // below is over the seeded DELTA: these exact rows appear, and these
      // exact rows are gone — which is the same claim an empty sweep makes,
      // stated about rows whose lifecycle this step controls.
      const before = await sweepInventoryWork(index);

      // Seed: a suspended run with a pending approval, and a keyed start
      // (a second suspended run PLUS a start reservation).
      const drained = await startFencedRun();
      const keyed = await startCountedWithKey('spike-inventory-key', 'inv');
      assert(
        keyed.status === 200 && keyed.body.status === 'suspended',
        'the keyed seed run suspended',
        keyed.body,
      );

      // Draining: the operator has stopped new work and is now proving what is
      // left. Reads stay open in every state, which is the whole point.
      await moveFence('open', 'draining');
      const seeded = await sweepInventoryWork(index);

      assert(
        seeded.runs.keys.some((key) => key.endsWith(`/${drained.runId}`)) &&
          seeded.runs.keys.some((key) => key.endsWith(`/${keyed.body.runId}`)),
        'both seeded runs are reported as outstanding work',
        seeded.runs,
      );
      assert(
        seeded['approvals-waiting'].keys.includes(drained.approvalId),
        'the pending approval is reported as outstanding work',
        seeded['approvals-waiting'],
      );
      assert(
        seeded['start-reservations'].keys.includes('spike-inventory-key'),
        'the unsettled start reservation is reported as outstanding work',
        seeded['start-reservations'],
      );
      assert(
        seeded.runs.count >= before.runs.count + 2,
        'the run count moved by exactly the work that was seeded',
        { before: before.runs.count, seeded: seeded.runs.count },
      );

      // The categories the earlier scenarios populated are READ here too, over
      // the same real D1: their sub-counts are what tell an operator a parked
      // task from one awaiting a webhook, and a due notification from one
      // scheduled for later.
      assert(
        typeof seeded['background-tasks'].count === 'number' &&
          typeof seeded['background-tasks'].totals?.fenceSuspended === 'number',
        'background tasks report a fence-parked sub-count',
        seeded['background-tasks'],
      );
      assert(
        typeof seeded['pending-notifications'].count === 'number' &&
          typeof seeded['pending-notifications'].totals?.notDue === 'number',
        'pending notifications separate what is due from what is not',
        seeded['pending-notifications'],
      );

      // Drain: a draining deployment still finishes what it already has. Both
      // seeded runs complete, and the reservation settles with its run.
      const decided = await http(
        'POST',
        `/api/approvals/${drained.approvalId}/decide`,
        { headers: AUTH.reviewer, body: { decision: 'approve' } },
      );
      assert(
        decided.status === 200 &&
          decided.body.resume?.summary?.status === 'success',
        'the draining deployment resumed the seeded run to success',
        decided,
      );
      const keyedApprovalId = keyed.body.approval?.id;
      assert(
        typeof keyedApprovalId === 'string',
        'the keyed seed run filed an approval to drain',
        keyed.body,
      );
      const keyedDecided = await http(
        'POST',
        `/api/approvals/${keyedApprovalId}/decide`,
        { headers: AUTH.reviewer, body: { decision: 'approve' } },
      );
      assert(
        keyedDecided.status === 200 &&
          keyedDecided.body.resume?.summary?.status === 'success',
        'the keyed seed run also resumed to success',
        keyedDecided,
      );

      // The proof: the seeded rows are gone, and STILL gone on a second full
      // sweep. One sweep is a point-in-time reading: traffic can still create
      // or complete work while it runs, so the contract asks for two.
      const gone = (sweep) =>
        !sweep.runs.keys.some((key) => key.endsWith(`/${drained.runId}`)) &&
        !sweep.runs.keys.some((key) => key.endsWith(`/${keyed.body.runId}`)) &&
        !sweep['approvals-waiting'].keys.includes(drained.approvalId) &&
        !sweep['approvals-waiting'].keys.includes(keyedApprovalId) &&
        !sweep['start-reservations'].keys.includes('spike-inventory-key');
      const first = await sweepInventoryWork(index);
      assert(gone(first), 'the first sweep no longer reports the seeded work', {
        runs: first.runs.keys,
        approvals: first['approvals-waiting'].keys,
        reservations: first['start-reservations'].keys,
      });
      const second = await sweepInventoryWork(index);
      assert(
        gone(second),
        'the second consecutive sweep agrees — the drain proof holds',
        {
          runs: second.runs.keys,
          approvals: second['approvals-waiting'].keys,
          reservations: second['start-reservations'].keys,
        },
      );

      // Standing configuration is NOT drained: a schedule and a provider
      // subscription survive the migration, and demanding they empty would make
      // the proof unreachable rather than strict.
      const schedules = await sweepInventoryCategory('schedules');
      const subscriptions = await sweepInventoryCategory(
        'signal-subscriptions',
      );
      assert(
        schedules.class === 'standing' && schedules.keys.length >= 1,
        'schedules are still reported while the drain proof passes',
        schedules,
      );
      assert(
        subscriptions.class === 'standing' && subscriptions.keys.length >= 1,
        'provider subscriptions are still reported while the proof passes',
        subscriptions,
      );

      // Lock: the proof held, so the deployment may stop executing. The
      // inventory keeps answering — reads are ungated in every state, which is
      // what lets an operator verify the lock they just took.
      await moveFence('draining', 'migration-locked');
      const locked = await sweepInventoryWork(index);
      assert(
        gone(locked),
        'the inventory still answers under the lock, with the same verdict',
        locked.runs.keys,
      );
      const refusedStart = await http('POST', '/runs', {
        body: RUN_BODY,
        headers: AUTH.operator,
      });
      assertFenced(
        'a run start after the drain proof locked the deployment',
        refusedStart,
        'migration-locked',
      );
      await moveFence('migration-locked', 'open');
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
      'Per-suspension deadlines (T1-T3): a real signal resumed its run as the ' +
      'human requester and settled the armed entry, a suspension carrying the ' +
      "reserved deadline key armed the run DO's own fenced wake, and after a " +
      'kill+restart that wake fired on the restarted object and resumed the run ' +
      'ITSELF with the timeout envelope under the system principal — no client ' +
      'called resume. The deployment execution fence (F1): provisioning seeded ' +
      'an explicit open fence, draining refused new starts with 503 ' +
      'EXECUTION_FENCED while still resuming an outstanding approval to ' +
      'success, migration-locked refused starts, raw resumes AND approval ' +
      'decisions (leaving the refused decision uncommitted), that lock survived ' +
      'a workerd kill+restart and still refused to mint, and reopening it ' +
      'restored minting and completed the approval the lock had refused. ' +
      'Owner-bound idempotent start (F3): a keyed start retried after a ' +
      'workerd kill+restart replayed the SAME run rather than starting a ' +
      'second, two genuinely concurrent same-key starts resolved to ONE ' +
      'run — both proved by a durable D1 execution counter reading exactly 1, ' +
      'with every concurrent refusal inside the published taxonomy — ' +
      'and under proof-only exactly the start carrying the nominated ' +
      'key was admitted and bound the fence to its run while unkeyed and ' +
      'wrong-keyed starts were refused. ' +
      'The drain inventory (F2): the index declared every category with its ' +
      'class, the Durable Object journal window it cannot see, and the rule an ' +
      'empty answer means something under; seeded runs, an approval, and a ' +
      'start reservation appeared as outstanding work while draining, left as ' +
      'each was finished, and read absent across TWO consecutive full sweeps ' +
      'paged through the keyset — after which the deployment locked, still ' +
      'answered the same inventory, and refused to mint, while its schedules ' +
      'and provider subscriptions kept being reported as standing ' +
      'configuration a migration carries rather than drains. ' +
      'Finally, a fresh Worker refused a D1 sentinel stamped for ' +
      'another deployment with 503 before authentication or routing.',
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
