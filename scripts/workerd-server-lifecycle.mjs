// SPDX-License-Identifier: Apache-2.0
// The one workerd dev-server lifecycle every local harness in this repository
// uses. It lives at the repository root, not inside a package, because its
// consumers include harnesses in DIFFERENT packages and
// .dependency-cruiser.cjs's `agent-starter-no-relative-package-reaches` rule
// forbids one of them from reaching into the other.
//
// Kill protocol: killing wrangler alone orphans its workerd child, which keeps
// serving the port and fakes persistence across a restart. So capture
// descendant PIDs BEFORE the kill (orphans reparent to init afterwards),
// SIGKILL the whole process group, poll the PIDs dead, and PROVE the port
// refuses before restarting; port-scoped `fuser -k <port>/tcp` is the last
// resort. Every syscall is injectable through `operations`, which is what lets
// workerd-server-lifecycle.test.mjs drive the protocol without real processes.
import { spawnSync } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import net from 'node:net';

const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
// Miniflare's outer proxy synthesizes this raw 500 when its user-Worker channel
// drops. Requiring its core stack prevents an application 500 with similar text
// from becoming retryable.
const NETWORK_CONNECTION_LOST = 'Error: Network connection lost.';
const MINIFLARE_CORE_ENTRY_FRAME =
  /^ {4}at async Object\.fetch \(file:\/\/\/.*[\\/]miniflare[\\/]dist[\\/]src[\\/]workers[\\/]core[\\/]entry\.worker\.js:\d+:\d+\)$/;

class RecoveryDeadlineError extends Error {}

export class WorkerdNetworkConnectionLostError extends Error {
  constructor(message) {
    super(message);
    this.name = 'WorkerdNetworkConnectionLostError';
  }
}

export function workerdNetworkConnectionLostResponseError(
  status,
  body,
  requestLabel,
) {
  const lines = typeof body === 'string' ? body.trimEnd().split(/\r?\n/) : [];
  if (
    status !== 500 ||
    lines.length !== 2 ||
    lines[0] !== NETWORK_CONNECTION_LOST ||
    !MINIFLARE_CORE_ENTRY_FRAME.test(lines[1])
  ) {
    return undefined;
  }
  return new WorkerdNetworkConnectionLostError(
    `${requestLabel} -> ${status} non-JSON: ${body.slice(0, 300)}`,
  );
}

export function parsePort(value, name = 'port') {
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new RangeError(`${name} must be an integer in 1..65535`);
  }
  return port;
}

function defaultPortState(port) {
  return new Promise((resolve) => {
    const socket = net.connect({ host: '127.0.0.1', port });
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

function defaultDescendantsOf(rootPid) {
  const childrenByParent = new Map();
  for (const entry of readdirSync('/proc')) {
    if (!/^\d+$/.test(entry)) continue;
    let stat;
    try {
      stat = readFileSync(`/proc/${entry}/stat`, 'utf8');
    } catch {
      continue;
    }
    const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
    const ppid = Number(fields[1]);
    const children = childrenByParent.get(ppid) ?? [];
    children.push(Number(entry));
    childrenByParent.set(ppid, children);
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

function defaultAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function defaultKillGroup(rootPid) {
  try {
    process.kill(-rootPid, 'SIGKILL');
  } catch {
    // The refusal proof below remains authoritative.
  }
}

function defaultKillPort(port) {
  spawnSync('fuser', ['-k', '-9', `${port}/tcp`], { stdio: 'ignore' });
}

async function defaultProbeHttp(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(2000) });
  await response.arrayBuffer();
}

export function createWorkerdServerLifecycle(options) {
  const port = parsePort(options.port);
  const ops = {
    sleep: options.operations?.sleep ?? defaultSleep,
    now: options.operations?.now ?? Date.now,
    portState: options.operations?.portState ?? (() => defaultPortState(port)),
    descendantsOf: options.operations?.descendantsOf ?? defaultDescendantsOf,
    alive: options.operations?.alive ?? defaultAlive,
    killGroup: options.operations?.killGroup ?? defaultKillGroup,
    killPort: options.operations?.killPort ?? (() => defaultKillPort(port)),
    setTimer: options.operations?.setTimer ?? setTimeout,
    clearTimer: options.operations?.clearTimer ?? clearTimeout,
    probeHttp:
      options.operations?.probeHttp ??
      (() => defaultProbeHttp(`http://127.0.0.1:${port}/`)),
  };
  let activeServer;

  const waitRefused = async (deadlineMs) => {
    const deadline = ops.now() + deadlineMs;
    let state = await ops.portState();
    while (state !== 'refused' && ops.now() < deadline) {
      await ops.sleep(250);
      state = await ops.portState();
    }
    return state;
  };

  const waitReady = async (server, deadlineMs) => {
    const deadline = ops.now() + deadlineMs;
    while (ops.now() < deadline) {
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
        await ops.probeHttp(server);
        if (child.exitCode !== null || child.signalCode !== null) {
          throw new Error(
            `${server.generation} exited while readiness was being checked`,
          );
        }
        return;
      } catch (error) {
        if (
          error instanceof Error &&
          error.message.includes('exited while readiness')
        ) {
          throw error;
        }
        await ops.sleep(500);
      }
    }
    throw new Error(`${server.generation} not ready within ${deadlineMs}ms`);
  };

  const retryNetworkConnectionLost = async (server, operation, deadlineMs) => {
    const deadline = ops.now() + deadlineMs;
    let lastError;
    const assertAlive = () => {
      const { child, spawnError } = server;
      if (spawnError) {
        throw new Error(`${server.generation} failed to spawn: ${spawnError}`);
      }
      if (child.exitCode !== null || child.signalCode !== null) {
        throw new Error(
          `${server.generation} exited while recovering network connection ` +
            `(code ${child.exitCode}, signal ${child.signalCode})`,
        );
      }
    };
    const attempt = async () => {
      const remainingMs = deadline - ops.now();
      if (remainingMs <= 0) throw new RecoveryDeadlineError();
      const controller = new AbortController();
      let timer;
      const timeout = new Promise((_, reject) => {
        timer = ops.setTimer(() => {
          controller.abort();
          reject(new RecoveryDeadlineError());
        }, remainingMs);
      });
      try {
        const result = await Promise.race([
          operation({ signal: controller.signal }),
          timeout,
        ]);
        if (ops.now() >= deadline) throw new RecoveryDeadlineError();
        return result;
      } finally {
        ops.clearTimer(timer);
      }
    };

    while (ops.now() < deadline) {
      assertAlive();
      try {
        const result = await attempt();
        assertAlive();
        return result;
      } catch (error) {
        assertAlive();
        if (error instanceof RecoveryDeadlineError) break;
        if (!(error instanceof WorkerdNetworkConnectionLostError)) {
          throw error;
        }
        lastError = error;
      }
      const remainingMs = deadline - ops.now();
      if (remainingMs <= 0) break;
      await ops.sleep(Math.min(250, remainingMs));
    }
    assertAlive();
    throw new Error(
      `${server.generation} network connection did not recover within ${deadlineMs}ms`,
      { cause: lastError },
    );
  };

  const stopServer = async (server) => {
    const rootPid = server.child.pid;
    const pids =
      rootPid === undefined ? [] : [rootPid, ...ops.descendantsOf(rootPid)];
    if (rootPid !== undefined) ops.killGroup(rootPid);
    const pidDeadline = ops.now() + 5000;
    let survivors = pids.filter(ops.alive);
    while (survivors.length > 0 && ops.now() < pidDeadline) {
      await ops.sleep(250);
      survivors = survivors.filter(ops.alive);
    }
    if (survivors.length > 0) {
      console.warn(`  pids still visible after kill: ${survivors.join(', ')}`);
    }
    let state = await waitRefused(5000);
    if (state !== 'refused') {
      ops.killPort();
      state = await waitRefused(5000);
    }
    if (state !== 'refused') {
      throw new Error(
        `port ${port} still ${state} after group kill + fuser ` +
          `(inspect: fuser -v ${port}/tcp; ss -ltnp 'sport = :${port}')`,
      );
    }
  };

  return {
    port,
    get activeServer() {
      return activeServer;
    },
    portState: ops.portState,
    async preflight() {
      const state = await ops.portState();
      if (state !== 'refused') {
        throw new Error(
          `port ${port} is already in use (state: ${state}); pick another port`,
        );
      }
    },
    async start(generation, launch, deadlineMs = 90_000) {
      if (activeServer !== undefined) {
        throw new Error('cannot start while another workerd server is active');
      }
      const server = launch();
      server.generation ??= generation;
      activeServer = server;
      await waitReady(server, deadlineMs);
      return server;
    },
    async retryNetworkConnectionLost(operation, deadlineMs = 5000) {
      if (activeServer === undefined) {
        throw new Error('cannot recover without an active workerd server');
      }
      // Callers opt in only operations whose response can be replayed safely.
      return retryNetworkConnectionLost(activeServer, operation, deadlineMs);
    },
    async requestJson(
      request,
      { requestLabel, replaySafe = false, deadlineMs = 5000 },
    ) {
      const send = async ({ signal } = {}) => {
        const response = await request(signal);
        const responseText = await response.text();
        const transient = workerdNetworkConnectionLostResponseError(
          response.status,
          responseText,
          requestLabel,
        );
        if (transient) throw transient;
        try {
          return { status: response.status, body: JSON.parse(responseText) };
        } catch {
          throw new Error(
            `${requestLabel} -> ${response.status} non-JSON: ${responseText.slice(0, 300)}`,
          );
        }
      };
      return replaySafe
        ? this.retryNetworkConnectionLost(send, deadlineMs)
        : send();
    },
    async stop() {
      if (activeServer === undefined) return;
      const server = activeServer;
      await stopServer(server);
      activeServer = undefined;
    },
    async cleanup(cleanOwnedResources) {
      let shutdownError;
      try {
        await this.stop();
      } catch (error) {
        shutdownError = error;
      }
      let cleanupError;
      try {
        await cleanOwnedResources?.();
      } catch (error) {
        cleanupError = error;
      }
      if (shutdownError) throw shutdownError;
      if (cleanupError) throw cleanupError;
    },
  };
}
