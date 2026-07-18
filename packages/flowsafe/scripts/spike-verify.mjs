// Automates the flowsafe workerd spike end-to-end so "durable execution
// across real process death" is a pass/fail command instead of a manual
// curl ritual (protocol: spike/worker.ts header). Scenario A starts a run
// that suspends at the approval gate, kills the dev server, restarts it
// on the SAME persisted state, decides the approval, and asserts the run
// resumed and published. Scenario B proves a forged raw resume (no grant
// minted) fails closed at the connector gate. The AG scenarios (Track A,
// M-002) prove the durable-agent approval-suspend shape (R-003) round-trips
// through the SAME grant-only path: the bridge derives the connector to grant
// from the agent's `toolName` (no explicit `connectors` array), an approval
// mints it, and a forged agent-gate resume fails closed at that gate.
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
import { spawn, spawnSync } from 'node:child_process';
import { createHmac } from 'node:crypto';
import {
  createWriteStream,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
} from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const FLOWSAFE = dirname(dirname(fileURLToPath(import.meta.url)));
const WRANGLER = join(FLOWSAFE, 'node_modules/.bin/wrangler');
const CONFIG = join(FLOWSAFE, 'spike/wrangler.jsonc');
const PORT = Number(process.env.SPIKE_VERIFY_PORT ?? 8799);
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
const AUTH = {
  admin: { authorization: 'Bearer spike-admin' },
  operator: { authorization: 'Bearer spike-operator' },
  reviewer: { authorization: 'Bearer spike-reviewer' },
  viewer: { authorization: 'Bearer spike-viewer' },
};

// Streaming (M-009). MUST equal spike/wrangler.jsonc `vars.STREAM_TICKET_SECRET`
// — startServer ALSO re-passes it via `--var` so the worker signs tickets with
// exactly this key. That lets the ticket probes below (a) prove a VALID forge is
// ACCEPTED (the positive control that makes the refusals meaningful — a secret
// mismatch would refuse everything and false-pass), and (b) craft EXPIRED and
// CROSS-TENANT tickets the worker must refuse at the CLAIM layer, not the
// signature. A LOCAL-ONLY spike fixture; never a real secret.
const STREAM_TICKET_SECRET = 'spike-local-stream-secret-do-not-deploy';
const nowSec = () => Math.floor(Date.now() / 1000);

// Track E (M-007): the github webhook signing secret. MUST equal
// spike/wrangler.jsonc `vars.GITHUB_WEBHOOK_SECRET`; startServer re-passes it via
// `--var` so the worker verifies with exactly this key, letting a VALID-signed
// webhook be ACCEPTED (the positive control) and a forged one REJECTED at the
// signature. A LOCAL-ONLY spike fixture; never a real secret.
const GITHUB_WEBHOOK_SECRET = 'spike-local-github-secret-do-not-deploy';

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

// 'listening' | 'refused' | 'timeout' | 'error:<code>' — only a clean
// ECONNREFUSED counts as free; anything else is treated as occupied.
function portState() {
  return new Promise((resolve) => {
    const socket = net.connect({ host: '127.0.0.1', port: PORT });
    let settled = false;
    const done = (state) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(state);
    };
    socket.setTimeout(1000, () => done('timeout'));
    socket.once('connect', () => done('listening'));
    socket.once('error', (error) => {
      done(error.code === 'ECONNREFUSED' ? 'refused' : `error:${error.code}`);
    });
  });
}

async function waitRefused(deadlineMs) {
  const deadline = Date.now() + deadlineMs;
  let state = await portState();
  while (state !== 'refused' && Date.now() < deadline) {
    await sleep(250);
    state = await portState();
  }
  return state;
}

// Transitive children of pid via /proc ppid links. Must run BEFORE the
// group kill: orphans reparent to init afterwards and become untraceable.
function descendantsOf(rootPid) {
  const childrenByParent = new Map();
  for (const entry of readdirSync('/proc')) {
    if (!/^\d+$/.test(entry)) continue;
    let stat;
    try {
      stat = readFileSync(`/proc/${entry}/stat`, 'utf8');
    } catch {
      continue; // process exited mid-scan
    }
    // format: pid (comm) state ppid ... — comm may contain spaces/parens
    const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
    const ppid = Number(fields[1]);
    if (!childrenByParent.has(ppid)) childrenByParent.set(ppid, []);
    childrenByParent.get(ppid).push(Number(entry));
  }
  const found = [];
  const queue = [rootPid];
  while (queue.length > 0) {
    const children = childrenByParent.get(queue.shift()) ?? [];
    found.push(...children);
    queue.push(...children);
  }
  return found;
}

function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
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

async function waitReady(server, deadlineMs) {
  const deadline = Date.now() + deadlineMs;
  while (Date.now() < deadline) {
    const { child, spawnError } = server;
    if (spawnError) {
      throw new Error(`${server.generation} failed to spawn: ${spawnError}`);
    }
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(
        `${server.generation} exited before ready ` +
          `(code ${child.exitCode}, signal ${child.signalCode})`,
      );
    }
    try {
      // Any HTTP response counts: the worker 404s unknown paths.
      await fetch(`${BASE}/`, { signal: AbortSignal.timeout(2000) });
      return;
    } catch {
      await sleep(500);
    }
  }
  throw new Error(`${server.generation} not ready within ${deadlineMs}ms`);
}

async function killServer(server) {
  const rootPid = server.child.pid;
  const pids =
    rootPid === undefined ? [] : [rootPid, ...descendantsOf(rootPid)];
  if (rootPid !== undefined) {
    try {
      process.kill(-rootPid, 'SIGKILL');
    } catch {
      // group already gone — the port checks below still apply
    }
  }
  const pidDeadline = Date.now() + 5000;
  let survivors = pids.filter(alive);
  while (survivors.length > 0 && Date.now() < pidDeadline) {
    await sleep(250);
    survivors = survivors.filter(alive);
  }
  if (survivors.length > 0) {
    console.warn(`  pids still visible after kill: ${survivors.join(', ')}`);
  }
  let state = await waitRefused(5000);
  if (state !== 'refused') {
    // Last resort, still scoped: kills exactly the port holder.
    spawnSync('fuser', ['-k', '-9', `${PORT}/tcp`], { stdio: 'ignore' });
    state = await waitRefused(5000);
  }
  if (state !== 'refused') {
    throw new Error(
      `port ${PORT} still ${state} after group kill + fuser ` +
        `(inspect: fuser -v ${PORT}/tcp; ss -ltnp 'sport = :${PORT}')`,
    );
  }
}

async function http(method, path, { body, headers } = {}) {
  const response = await fetch(`${BASE}${path}`, {
    method,
    headers: {
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
      ...headers,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
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

// Forge a stream ticket with the SAME primitives as the worker's
// mintStreamTicket: base64url(JSON(claims)) + '.' + HMAC-SHA256 over that
// payload (base64url). crypto.createHmac(...).digest('base64url') is byte-equal
// to the worker's WebCrypto hmacSign, so a VALID forge is ACCEPTED (positive
// control) and EXPIRED / CROSS-TENANT forges exercise the worker's claim
// validation, not a signature mismatch.
function forgeTicket(claims) {
  const payload = Buffer.from(JSON.stringify(claims), 'utf8').toString(
    'base64url',
  );
  const signature = createHmac('sha256', STREAM_TICKET_SECRET)
    .update(payload)
    .digest('base64url');
  return `${payload}.${signature}`;
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
    const settle = (fn, arg) => {
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
  try {
    if (currentServer !== undefined) {
      const server = currentServer;
      currentServer = undefined;
      await killServer(server);
    }
  } finally {
    for (const server of servers) {
      server.file.end();
    }
    if (tmpDir !== undefined) {
      rmSync(tmpDir, { recursive: true, force: true });
      tmpDir = undefined;
    }
  }
}

async function main() {
  if (FAULT !== undefined && FAULT !== 'skip-decide') {
    throw new Error(
      `unknown SPIKE_VERIFY_FAULT '${FAULT}' (supported: skip-decide)`,
    );
  }

  await step(`preflight: port ${PORT} must be free`, async () => {
    const state = await portState();
    assert(
      state === 'refused',
      `port ${PORT} is already in use (state: ${state}); pick another with ` +
        `SPIKE_VERIFY_PORT=<port>, or inspect with: fuser -v ${PORT}/tcp`,
    );
  });

  tmpDir = mkdtempSync(join(tmpdir(), 'spike-verify-'));
  const stateDir = join(tmpDir, 'state');

  currentServer = startServer('gen-1', stateDir, join(tmpDir, 'gen1.log'));
  await step('gen-1: server ready', () => waitReady(currentServer, 90_000));

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
        body.runId.startsWith('spike_'),
        'runId carries the tenant prefix (INV-1: server-minted `spike_<uuid>`)',
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
    currentServer = undefined;
  });

  currentServer = startServer('gen-2', stateDir, join(tmpDir, 'gen2.log'));
  await step('A2 restart: gen-2 on the same persisted state', async () => {
    await waitReady(currentServer, 90_000);
    assert(
      !/address already in use/i.test(currentServer.chunks.join('')),
      'gen-2 log must not contain "address already in use" (orphan trap)',
    );
  });

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
        'approval required and not granted',
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
      // the request to the system actor. admin is the only role that can
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
          'approval required and not granted',
        ),
        'agent gate error names the missing grant',
        forged.body.error,
      );
    },
  );

  // --- Part B live streaming (M-009): real WebSockets over workerd ----------
  // Each probe below opens an ACTUAL WebSocket against wrangler dev and asserts
  // something that can FAIL (and exit non-zero): a fanned-out event is received,
  // a cross-tenant socket stays silent, a subscription survives a kill+restart,
  // and expired/cross-tenant/garbage/cross-channel tickets are refused.

  await step(
    'D fan-out + hub cross-tenant isolation: a decided event reaches the ' +
      "tenant's socket and never another tenant's",
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

      // A forged tenant-'other' hub ticket (valid signature via the shared local
      // secret) subscribes to tenant OTHER's hub, NEVER spike's.
      const otherTicket = forgeTicket({
        tenantId: 'other',
        channel: 'hub',
        actorId: 'mallory',
        role: 'reviewer',
        exp: nowSec() + 60,
      });
      const other = connectWs('/api/stream/hub', otherTicket);
      await waitOpen(other, 10_000);

      // Decide the spike approval -> fires a 'decided' stream event (tenant
      // spike), fanned out over the hub to spike's sockets only.
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
        frame.event.record.tenantId === 'spike',
        'fanned-out event is tenant spike',
        frame.event.record,
      );
      assert(
        frame.event.record.id === approvalId,
        'fanned-out event is the decided approval',
        frame.event.record,
      );

      // Cross-tenant isolation: give any erroneous leak time to arrive (the
      // spike frame already proved the event fired), then assert the 'other'
      // socket saw NO spike queue frame.
      await sleep(500);
      const leaked = other.frames.find((f) => f?.type === 'queue');
      assert(
        leaked === undefined,
        'LEAK: tenant other received a spike queue event',
        leaked,
      );

      spike.close();
      other.close();
    },
  );

  await step(
    'E hibernation persistence: a re-opened subscription still receives ' +
      'events across a workerd kill+restart',
    async () => {
      await killServer(currentServer);
      currentServer = undefined;
      currentServer = startServer('gen-3', stateDir, join(tmpDir, 'gen3.log'));
      await waitReady(currentServer, 90_000);
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
        frame.event.record.tenantId === 'spike',
        'post-restart fan-out is tenant spike',
        frame.event.record,
      );
      sock.close();
    },
  );

  await step(
    'F ticket fail-closed: a valid forge opens, but expired / cross-tenant / ' +
      'garbage / cross-channel tickets are refused',
    async () => {
      const spikeRunId = run.runId; // a real spike-owned runId (from A1)

      // POSITIVE CONTROL: a forged-but-VALID hub ticket (correct secret, future
      // exp) OPENS. Proves the shared local secret matches the worker's, so the
      // refusals below are CLAIM rejections, not signature drift. If the secret
      // ever drifts, THIS throws and the probe fails loudly.
      const valid = forgeTicket({
        tenantId: 'spike',
        channel: 'hub',
        actorId: 'vic',
        role: 'viewer',
        exp: nowSec() + 60,
      });
      const control = connectWs('/api/stream/hub', valid);
      await waitOpen(control, 10_000);
      control.close();

      // Expired hub ticket -> refused (exp in the past).
      const expired = forgeTicket({
        tenantId: 'spike',
        channel: 'hub',
        actorId: 'vic',
        role: 'viewer',
        exp: nowSec() - 30,
      });
      await expectWsRefused('/api/stream/hub', expired, 8_000);

      // Cross-tenant RUN ticket: tenant 'other' claiming a SPIKE runId ->
      // refused (tenantOwnsSaltedId('other', spikeRunId) is false).
      const crossTenant = forgeTicket({
        tenantId: 'other',
        channel: 'run',
        runId: spikeRunId,
        actorId: 'mallory',
        role: 'reviewer',
        exp: nowSec() + 60,
      });
      await expectWsRefused(
        `/api/stream/run/demo-approval/${spikeRunId}`,
        crossTenant,
        8_000,
      );

      // Garbage signature -> refused (signature validation).
      await expectWsRefused('/api/stream/hub', 'forged.signature', 8_000);

      // Cross-channel: a hub ticket presented on the run route -> refused.
      const hubForRun = forgeTicket({
        tenantId: 'spike',
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
      // pubsub-keyed registry (no LLM — Track A's real-loop drive is deferred).
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
    'J cross-tenant fail-closed (C-S4): a foreign-threadId send is refused at ' +
      'BOTH the topology ownership 404 and the DO header 403',
    async () => {
      const { status, body } = await http('POST', '/sig/cross-tenant');
      assert(status === 200, `sig cross-tenant probe -> ${status}`, body);
      // Barrier 1: the topology 404s the foreign threadId BEFORE the DO is
      // addressed — no wake, no existence oracle.
      assert(
        body.ownershipStatus === 404,
        'topology ownership 404 (tenant does not own the threadId)',
        body,
      );
      // Barrier 2: a DIRECT forged-header fetch (bypassing the topology) is 403'd
      // by the DO's own #assertTenantIdentity (name tenant != forged header).
      assert(
        body.headerStatus === 403,
        'DO header assertion 403 (a forged x-flowsafe-tenant cannot pass)',
        body,
      );
    },
  );

  // --- Track F goals (M-005) ------------------------------------------------
  // Prove the goal objective surface writes the mastra_thread_state domain and
  // that the DURABLE goal-step read path (resolveGoalStore -> readObjective over
  // the COMPOSED storage) reads it back — including across a workerd restart (the
  // DO-eviction analog). No LLM: the real goal LOOP is Track A's deferred residual.
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
    'L goal fail-closed (F-S3): a cross-tenant write is 404 + audited, and an ' +
      'over-cap maxRuns is rejected + audited',
    async () => {
      const cross = await http('POST', '/goal/cross-tenant');
      assert(
        cross.status === 200,
        `cross-tenant probe -> ${cross.status}`,
        cross.body,
      );
      assert(
        cross.body.status === 404,
        'a foreign-threadId objective write is 404 (no existence oracle)',
        cross.body,
      );
      assert(
        cross.body.audited.includes('rejected:foreign-thread'),
        'the cross-tenant write was audited',
        cross.body.audited,
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
      currentServer = undefined;
      currentServer = startServer('gen-4', stateDir, join(tmpDir, 'gen4.log'));
      await waitReady(currentServer, 90_000);
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
  // core's pubsub worker loop. The two load-bearing proofs: exactly-once under
  // concurrent ticks (the CAS), and the stored-context barrier + INV-1 mint on a
  // real DO fire.
  await step(
    'N schedule exactly-once (D-S1): two CONCURRENT ticks over one due schedule ' +
      'fire EXACTLY once (CAS), one trigger row, nextFireAt advanced once',
    async () => {
      const { status, body } = await http('POST', '/sched/exactly-once');
      assert(status === 200, `sched exactly-once probe -> ${status}`, body);
      // One of the two concurrent ticks won the CAS; the other lost — proving the
      // updateScheduleNextFire compare-and-swap serializes the claim on real D1.
      assert(
        body.fires === 1,
        'exactly ONE fire across two concurrent ticks',
        body,
      );
      assert(body.lost === 1, 'the OTHER tick lost the CAS claim', body);
      assert(body.fireCount === 1, 'the start seam ran exactly once', body);
      assert(body.published === 1, 'exactly one published trigger row', body);
      assert(
        body.advanced === true,
        'nextFireAt advanced once (the fire is consumed, never hot-looped)',
        body,
      );
    },
  );

  await step(
    'O schedule barrier + INV-1 (D-S2): a workflow schedule fires through ' +
      'RunnerRuntime with a fresh INV-1 runId and the stored-context barrier holds',
    async () => {
      const { status, body } = await http('POST', '/sched/barrier');
      assert(status === 200, `sched barrier probe -> ${status}`, body);
      assert(body.fired === 1, 'the workflow target fired', body);
      // INV-1: the tick minted a tenant-salted `spike_<uuid>` runId, never a bare
      // crypto.randomUUID.
      assert(
        typeof body.runId === 'string' && body.runId.startsWith('spike_'),
        'the fired runId is INV-1 (<tenantId>_<uuid>)',
        body,
      );
      assert(
        body.status === 'success',
        'the DO ran sched-echo through RunnerRuntime to completion',
        body,
      );
      // The barrier: the FORGED connector id planted in the schedule ROW's stored
      // requestContext never becomes a grant on the executing leg — the topology
      // start carries only inputData (stored context dropped), and the DO's own
      // approvalGrantProvider mints an EMPTY grant list that wins. (The grant KEY
      // is legitimately present as []; the forged VALUE is what must not appear.)
      assert(
        body.leg?.reservedLeaked === false,
        'the forged connector planted in the row did NOT become a grant (barrier holds)',
        body.leg,
      );
      // ...while the DO's OWN scope keys (minted by #requestContextFor) ARE
      // present — proving the run went through RunnerRuntime, not a forked path.
      assert(
        body.leg?.workflowScopePresent === true &&
          body.leg?.isolationScopePresent === true,
        'the runtime-derived scope keys ARE present (ran through RunnerRuntime)',
        body.leg,
      );
      // and the stored NON-reserved key is dropped on the DO path (the topology
      // start seam carries only inputData) — strictly more fail-closed.
      assert(
        body.leg?.customPresent === false,
        'stored non-reserved context is dropped on the DO path (fail-closed)',
        body.leg,
      );
    },
  );

  // --- Track E signal providers (M-007) -------------------------------------
  // Webhooks terminate on the Worker, verified BEFORE parse; delivery routes
  // through the topology into the thread inbox; a poll provider survives DO
  // eviction by rehydrating its subscriptions from D1; the ROW is the sole
  // tenant authority (a tampered/foreign row fails closed at the topology).
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
      const inbox = await http(
        'GET',
        '/sigp/notifications?threadId=spike_sigp',
      );
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
      const inbox = await http(
        'GET',
        '/sigp/notifications?threadId=spike_sigp',
      );
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
      currentServer = undefined;
      currentServer = startServer('gen-5', stateDir, join(tmpDir, 'gen5.log'));
      await waitReady(currentServer, 90_000);
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
      const inbox = await http(
        'GET',
        '/sigp/notifications?threadId=spike_sigp',
      );
      assert(
        inbox.body.sources.includes('spike-poller'),
        'the poll notification landed in the thread inbox after the restart',
        inbox.body,
      );
    },
  );

  await step(
    'S cross-tenant fail-closed: a VALID-signed webhook for a TAMPERED row ' +
      '(tenant spike, a thread it does not own) delivers to NO thread',
    async () => {
      const tamper = await http('POST', '/sigp/subscribe-foreign');
      assert(
        tamper.status === 200 && tamper.body.tampered === true,
        'a tampered row was created (tenant spike, foreign thread)',
        tamper.body,
      );
      const webhook = await postWebhook('github', {
        repository: { full_name: 'cross/repo' },
      });
      assert(
        webhook.status === 200,
        `cross webhook -> HTTP ${webhook.status}`,
        webhook.body,
      );
      assert(
        webhook.body.matched === 1,
        'the tampered row matched by resource (the row IS the authority)',
        webhook.body,
      );
      // The topology ownership check 404s the foreign threadId — delivered to none.
      assert(
        webhook.body.delivered === 0,
        'delivery to the foreign thread was 404 by the topology (fail closed)',
        webhook.body,
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
    '\nSPIKE VERIFIED: run survived process death, resumed via approval ' +
      'grant, a forged raw resume failed closed, and self-decision was denied. ' +
      'Live streaming (M-009): a decided event fanned out to the tenant socket, ' +
      'stayed isolated from another tenant, survived a kill+restart, and ' +
      'refused expired / cross-tenant / garbage / cross-channel tickets. ' +
      'Track B (M-003): a smuggled _background arg was rejected + audited ' +
      '(B-S3), and a fresh init() recovered a task left running in D1 (B-S2). ' +
      'Track C (M-004): a send into an ACTIVE thread-DO loop drained IN-PROCESS ' +
      'via the shared-pubsub registry (C-S2, the DL-002 affinity thesis), and a ' +
      'foreign-threadId send failed closed at BOTH the topology 404 and the DO ' +
      'header 403 (C-S4). Track F (M-005): an objective set via the route landed ' +
      'in mastra_thread_state and the durable goal-step read path returned it ' +
      '(F-S1), survived a kill+restart (F-S2), and a cross-tenant write + over-cap ' +
      'maxRuns failed closed and audited (F-S3). Track D (M-006): two concurrent ' +
      'ticks over one due schedule fired EXACTLY once via the CAS (D-S1), and a ' +
      'workflow schedule fired through RunnerRuntime with a fresh INV-1 runId ' +
      'while a reserved key planted in the row stayed ABSENT from the leg (D-S2). ' +
      'Track E (M-007): a forged webhook was rejected BEFORE parse and audited ' +
      '(E-S2), a signed webhook matched its subscription row and landed a ' +
      'notification in the thread inbox (E-S1), a poll provider rehydrated its ' +
      'subscriptions from D1 after a kill+restart and fired delivery (E-S3), and ' +
      'a valid webhook for a tampered foreign-thread row delivered to NONE ' +
      '(cross-tenant fail-closed at the topology).',
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
