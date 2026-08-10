import { build } from 'vite';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createTestHarness,
  type TestHarness,
  type WorkerHandle,
} from 'wrangler';

const DEPLOYMENT_SECRET = 'test-deployment-identity-secret-0001';
const STREAM_SECRET = 'test-stream-ticket-secret-0000001';
const TOKENS = {
  'tok-admin': { id: 'ada', role: 'admin' },
  'tok-reviewer': { id: 'ray', role: 'reviewer' },
};
const DATABASE = {
  binding: 'DB',
  database_name: 'anchorage-showcase-single-tenant',
  database_id: '00000000-0000-0000-0000-000000000000',
};
const ROOT = new URL('..', import.meta.url).pathname;
const BUILT_CONFIG = new URL(
  '../dist/anchorage_showcase_single_tenant/wrangler.json',
  import.meta.url,
);

function harnessOptions(streamTicketSecret: string) {
  return {
    root: ROOT,
    workers: [
      {
        // The Vite output config is the production snapshot Wrangler uses for
        // preview/deploy; unlike the input config, it owns assets.directory.
        configPath: BUILT_CONFIG,
        secrets: {
          DEPLOYMENT_IDENTITY_SECRET: DEPLOYMENT_SECRET,
          APPROVAL_ACTOR_TOKENS: JSON.stringify(TOKENS),
          STREAM_TICKET_SECRET: streamTicketSecret,
          DEMO_JWT_SECRET: '',
          GOOGLE_CLIENT_SECRET: '',
        },
      },
      {
        config: {
          name: 'showcase-harness-seeder',
          main: new URL('../test-support/harness-seeder.ts', import.meta.url)
            .pathname,
          compatibility_date: '2026-07-26',
          d1_databases: [DATABASE],
        },
      },
    ],
  } satisfies Parameters<typeof createTestHarness>[0];
}

interface HarnessEnv {
  DB: D1Database;
  RUNNER: DurableObjectNamespace;
  HUB: DurableObjectNamespace;
}

type Worker = WorkerHandle<HarnessEnv>;

function authenticated(token: keyof typeof TOKENS, body?: unknown) {
  return {
    method: body === undefined ? 'GET' : 'POST',
    headers: {
      authorization: `Bearer ${token}`,
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  };
}

class WebSocketProbe {
  readonly #frames: unknown[] = [];
  readonly #socket: WebSocket;
  readonly opened: Promise<void>;

  constructor(base: URL, path: string, ticket: string) {
    const url = new URL(path, base);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    url.searchParams.set('ticket', ticket);
    this.#socket = new WebSocket(url.toString());
    this.#socket.addEventListener('message', (event) => {
      if (typeof event.data !== 'string') return;
      try {
        this.#frames.push(JSON.parse(event.data));
      } catch {
        // The production protocol is JSON-only. An invalid frame must not make
        // the probe lose a later valid frame and hide the protocol failure.
        this.#frames.push(event.data);
      }
    });
    this.opened = new Promise((resolve, reject) => {
      this.#socket.addEventListener('open', () => resolve(), { once: true });
      this.#socket.addEventListener(
        'error',
        () => reject(new Error(`WebSocket failed to open: ${url}`)),
        { once: true },
      );
    });
  }

  async waitFor<T>(
    predicate: (frame: unknown) => frame is T,
    timeoutMs = 5_000,
  ): Promise<T> {
    const existing = this.#frames.find(predicate);
    if (existing !== undefined) return existing;

    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        cleanup();
        reject(
          new Error(
            `timed out waiting for WebSocket frame; received ${JSON.stringify(this.#frames)}`,
          ),
        );
      }, timeoutMs);
      const onMessage = () => {
        const frame = this.#frames.find(predicate);
        if (frame === undefined) return;
        cleanup();
        resolve(frame);
      };
      const onClose = (event: CloseEvent) => {
        cleanup();
        reject(
          new Error(
            `WebSocket closed before expected frame (${event.code}: ${event.reason})`,
          ),
        );
      };
      const cleanup = () => {
        clearTimeout(timeout);
        this.#socket.removeEventListener('message', onMessage);
        this.#socket.removeEventListener('close', onClose);
      };
      this.#socket.addEventListener('message', onMessage);
      this.#socket.addEventListener('close', onClose, { once: true });
    });
  }

  close(): void {
    this.#socket.close();
  }
}

interface StreamTicket {
  url: string;
  ticket: string;
}

interface RunSummary {
  runId: string;
  status: string;
  result?: unknown;
}

interface StartedRun extends RunSummary {
  approval: { id: string; status: string };
}

async function ticket(
  worker: Worker,
  token: keyof typeof TOKENS,
  body: unknown,
): Promise<StreamTicket> {
  const response = await worker.fetch(
    '/api/stream/ticket',
    authenticated(token, body),
  );
  expect(response.status).toBe(200);
  return (await response.json()) as StreamTicket;
}

describe.sequential('showcase Wrangler test harness', () => {
  let server: TestHarness;
  let worker: Worker;
  let baseUrl: URL;
  let completedRun: StartedRun;

  beforeAll(async () => {
    await build({ root: ROOT, logLevel: 'silent' });
    server = createTestHarness(harnessOptions(STREAM_SECRET));
    ({ url: baseUrl } = await server.listen());
    worker = server.getWorker<HarnessEnv>();

    const seeded = await server
      .getWorker('showcase-harness-seeder')
      .fetch('/seed', { method: 'POST' });
    expect(seeded.status).toBe(200);
  }, 30_000);

  afterAll(async () => {
    await server.close();
  });

  it('loads the real Wrangler config and serves the actual Worker over its D1 deployment boundary', async () => {
    const health = await worker.fetch('/healthz');
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ ok: true });

    const catalog = await worker.fetch(
      '/workflows',
      authenticated('tok-admin'),
    );
    expect(catalog.status).toBe(200);
    expect(await catalog.json()).toEqual(
      expect.objectContaining({
        actor: { id: 'ada', role: 'admin', canSelfDecide: true },
        workflows: expect.arrayContaining([
          expect.objectContaining({ id: 'gtm-outbound' }),
        ]),
      }),
    );
  });

  it('upgrades the real hub and runner Durable Objects and delivers their authoritative events', async () => {
    const hubTicket = await ticket(worker, 'tok-reviewer', { channel: 'hub' });
    const hub = new WebSocketProbe(baseUrl, hubTicket.url, hubTicket.ticket);
    await hub.opened;

    const presence = await hub.waitFor(
      (frame): frame is { type: 'presence'; roster: unknown[] } =>
        typeof frame === 'object' &&
        frame !== null &&
        (frame as { type?: unknown }).type === 'presence',
    );
    expect(presence.roster).toEqual([{ actorId: 'ray', role: 'reviewer' }]);

    const start = await worker.fetch(
      '/runs',
      authenticated('tok-admin', {
        workflowId: 'gtm-outbound',
        inputData: { industry: 'fintech', targetCount: 2 },
      }),
    );
    expect(start.status).toBe(200);
    const started = (await start.json()) as StartedRun;
    expect(started).toMatchObject({
      status: 'suspended',
      approval: { status: 'pending' },
    });

    const queueFrame = await hub.waitFor(
      (
        frame,
      ): frame is {
        type: 'queue';
        event: { type: string; record: { id: string; runId: string } };
      } =>
        typeof frame === 'object' &&
        frame !== null &&
        (frame as { type?: unknown }).type === 'queue',
    );
    expect(queueFrame.event).toMatchObject({
      type: 'created',
      record: { id: started.approval.id, runId: started.runId },
    });
    hub.close();

    const reconnectedHubTicket = await ticket(worker, 'tok-reviewer', {
      channel: 'hub',
    });
    const reconnectedHub = new WebSocketProbe(
      baseUrl,
      reconnectedHubTicket.url,
      reconnectedHubTicket.ticket,
    );
    await reconnectedHub.opened;
    const reconnectedPresence = await reconnectedHub.waitFor(
      (frame): frame is { type: 'presence'; roster: unknown[] } =>
        typeof frame === 'object' &&
        frame !== null &&
        (frame as { type?: unknown }).type === 'presence',
    );
    expect(reconnectedPresence.roster).toEqual([
      { actorId: 'ray', role: 'reviewer' },
    ]);

    const runTicket = await ticket(worker, 'tok-admin', {
      channel: 'run',
      workflowId: 'gtm-outbound',
      runId: started.runId,
    });
    const run = new WebSocketProbe(baseUrl, runTicket.url, runTicket.ticket);
    await run.opened;
    const snapshot = await run.waitFor(
      (frame): frame is { type: 'run'; summary: RunSummary } =>
        typeof frame === 'object' &&
        frame !== null &&
        (frame as { type?: unknown }).type === 'run',
    );
    expect(snapshot.summary).toMatchObject({
      runId: started.runId,
      status: 'suspended',
    });
    run.close();

    const reconnectedRunTicket = await ticket(worker, 'tok-admin', {
      channel: 'run',
      workflowId: 'gtm-outbound',
      runId: started.runId,
    });
    const reconnectedRun = new WebSocketProbe(
      baseUrl,
      reconnectedRunTicket.url,
      reconnectedRunTicket.ticket,
    );
    await reconnectedRun.opened;
    const reconnectedSnapshot = await reconnectedRun.waitFor(
      (frame): frame is { type: 'run'; summary: RunSummary } =>
        typeof frame === 'object' &&
        frame !== null &&
        (frame as { type?: unknown }).type === 'run',
    );
    expect(reconnectedSnapshot.summary).toMatchObject({
      runId: started.runId,
      status: 'suspended',
    });

    const claimed = await worker.fetch(
      `/api/approvals/${started.approval.id}/claim`,
      authenticated('tok-reviewer', {}),
    );
    expect(claimed.status).toBe(200);

    const decided = await worker.fetch(
      `/api/approvals/${started.approval.id}/decide`,
      authenticated('tok-reviewer', { decision: 'approve' }),
    );
    expect(decided.status).toBe(200);

    const terminal = await reconnectedRun.waitFor(
      (frame): frame is { type: 'run'; summary: RunSummary } =>
        typeof frame === 'object' &&
        frame !== null &&
        (frame as { type?: unknown }).type === 'run' &&
        (frame as { summary?: { status?: unknown } }).summary?.status ===
          'success',
    );
    expect(terminal.summary).toMatchObject({
      runId: started.runId,
      status: 'success',
      result: { outcome: 'simulated', delivered: 0 },
    });

    expect(await worker.listDurableObjectIds('RUNNER')).toHaveLength(1);
    expect(await worker.listDurableObjectIds('HUB')).toHaveLength(1);
    completedRun = { ...started, ...terminal.summary };
    reconnectedHub.close();
    reconnectedRun.close();
  }, 15_000);

  it('keeps polling mounted and streaming unmounted when the stream secret is absent', async () => {
    await server.update(harnessOptions(''));

    const stream = await worker.fetch(
      '/api/stream/ticket',
      authenticated('tok-admin', { channel: 'hub' }),
    );
    expect(stream.status).toBe(404);
    expect(await stream.json()).toEqual({ error: 'not found' });

    const runPoll = await worker.fetch(
      `/runs/gtm-outbound/${completedRun.runId}`,
      authenticated('tok-admin'),
    );
    expect(runPoll.status).toBe(200);
    expect(await runPoll.json()).toMatchObject({
      runId: completedRun.runId,
      status: 'success',
    });

    const queuePoll = await worker.fetch(
      '/api/approvals',
      authenticated('tok-reviewer'),
    );
    expect(queuePoll.status).toBe(200);
  }, 15_000);
});
