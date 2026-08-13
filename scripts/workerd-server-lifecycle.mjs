// SPDX-License-Identifier: Apache-2.0
// The one workerd dev-server lifecycle every local harness in this repository
// uses. It lives at the repository root, not inside a package, because its two
// consumers are harnesses in DIFFERENT packages and
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
  await fetch(url, { signal: AbortSignal.timeout(2000) });
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
