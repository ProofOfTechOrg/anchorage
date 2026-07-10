// Automates the flowsafe workerd spike end-to-end so "durable execution
// across real process death" is a pass/fail command instead of a manual
// curl ritual (protocol: spike/worker.ts header). Scenario A starts a run
// that suspends at the approval gate, kills the dev server, restarts it
// on the SAME persisted state, decides the approval, and asserts the run
// resumed and published. Scenario B proves a forged raw resume (no grant
// minted) fails closed at the connector gate.
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
const AUTH = {
  admin: { authorization: 'Bearer spike-admin' },
  operator: { authorization: 'Bearer spike-operator' },
  reviewer: { authorization: 'Bearer spike-reviewer' },
  viewer: { authorization: 'Bearer spike-viewer' },
};

let currentStep = 'startup';
let currentServer;
let tmpDir;
const servers = [];

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

function dumpLog(server) {
  const tail = server.chunks.join('').split('\n').slice(-200);
  console.error(`\n--- ${server.generation} log tail (${server.logPath}) ---`);
  console.error(tail.join('\n'));
}

// Even when the kill fails (port won't free), release what we own —
// log fds and the temp dir — before the failure propagates.
async function cleanup() {
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
      'grant, a forged raw resume failed closed, and self-decision was denied.',
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
