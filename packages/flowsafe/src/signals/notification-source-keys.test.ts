// SPDX-License-Identifier: Apache-2.0

import { Agent } from '@mastra/core/agent';
import type { MastraModelConfig } from '@mastra/core/llm';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import type {
  NotificationDeliveryPolicyConfig,
  NotificationDeliveryPolicyDecision,
  NotificationPriority,
  NotificationRecord,
  NotificationStatus,
} from '@mastra/core/notifications';
import {
  notificationSummaryContents,
  notificationSummarySignalMetadata,
  resolveNotificationDeliveryDecision,
  summarizeNotifications,
} from '@mastra/core/notifications';
import { InMemoryStore } from '@mastra/core/storage';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

declare const process: {
  on(event: 'unhandledRejection', listener: (reason: unknown) => void): void;
  off(event: 'unhandledRejection', listener: (reason: unknown) => void): void;
};

let unhandled: unknown[];
let onUnhandled: (reason: unknown) => void;

// The thread stream runtime is a module singleton, so a spy on it outlives the
// case that installed it. An unconsumed stream's rejection outlives it too.
beforeEach(() => {
  unhandled = [];
  onUnhandled = (reason) => unhandled.push(reason);
  process.on('unhandledRejection', onUnhandled);
});

afterEach(() => {
  process.off('unhandledRejection', onUnhandled);
  vi.restoreAllMocks();
});

const getBuiltin = (
  globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }
).process?.getBuiltinModule;
if (!getBuiltin) throw new Error('CJS coverage requires node >= 22.3');
const { createRequire } = getBuiltin('node:module') as {
  createRequire: (from: string) => (id: string) => unknown;
};
const cjs = createRequire((import.meta as unknown as { url: string }).url)(
  '@mastra/core/notifications',
) as {
  summarizeNotifications: typeof summarizeNotifications;
  notificationSummaryContents: typeof notificationSummaryContents;
  notificationSummarySignalMetadata: typeof notificationSummarySignalMetadata;
  resolveNotificationDeliveryDecision: typeof resolveNotificationDeliveryDecision;
};

const esm = {
  summarizeNotifications,
  notificationSummaryContents,
  notificationSummarySignalMetadata,
  resolveNotificationDeliveryDecision,
};

const modules: Array<[string, typeof esm]> = [
  ['esm', esm],
  ['cjs', cjs],
];

// The getting-started guide's confirmation record. With the patch applied,
// `bySource.constructor` carries this record's own count instead of the
// inherited Object.prototype member.
const PATCH_PROBE: NotificationRecord = {
  id: 'n',
  threadId: 't',
  source: 'constructor',
  kind: 'k',
  priority: 'low',
  status: 'pending',
  summary: 's',
  createdAt: new Date(0),
  updatedAt: new Date(0),
};

const patched = new Map(
  modules.map(
    ([label, core]) =>
      [
        label,
        typeof core.summarizeNotifications([PATCH_PROBE]).bySource
          .constructor === 'number',
      ] as const,
  ),
);

it('loads the CJS lane as its own module realization', () => {
  expect(cjs.summarizeNotifications).not.toBe(summarizeNotifications);
});

it.each(
  modules,
)('reports the @mastra/core patch applied (%s)', (_label, core) => {
  expect(
    typeof core.summarizeNotifications([PATCH_PROBE]).bySource.constructor,
  ).toBe('number');
});

const CREATED_AT = new Date('2026-01-01T00:00:00.000Z');

function record(
  id: string,
  source: string,
  priority: NotificationPriority,
  status: NotificationStatus = 'pending',
): NotificationRecord {
  return {
    id,
    threadId: 'thread-keys',
    resourceId: 'resource-keys',
    agentId: 'agent-keys',
    source,
    kind: 'changed',
    priority,
    status,
    summary: `summary ${id}`,
    createdAt: CREATED_AT,
    updatedAt: CREATED_AT,
  };
}

/**
 * Object literals treat a `__proto__` key as the prototype setter, so a literal
 * can never express the own entry these cases are about.
 */
function sourcePolicy(
  entries: Array<[string, NotificationDeliveryPolicyDecision]>,
): Record<string, NotificationDeliveryPolicyDecision> {
  return Object.fromEntries(entries);
}

const BATCH: NotificationRecord[] = [
  record('n1', '__proto__', 'low'),
  record('n2', 'constructor', 'medium'),
  record('n3', 'toString', 'high'),
  record('n4', 'hasOwnProperty', 'urgent'),
  record('n5', '', 'low'),
  record('n6', 'crm', 'low'),
  record('n7', 'crm', 'medium'),
  record('n8', 'constructor', 'low', 'delivered'),
];

const POLICY_SOURCES = [
  'ordinary',
  '__proto__',
  'constructor',
  'toString',
  'hasOwnProperty',
] as const;

function unreachableModel() {
  const unreachable = () =>
    Promise.reject(new Error('source-key tests must not reach a model'));
  const doGenerate = vi.fn(unreachable);
  const doStream = vi.fn(unreachable);
  const model: MastraModelConfig = {
    specificationVersion: 'v2',
    provider: 'flowsafe-test',
    modelId: 'unreachable',
    supportedUrls: {},
    doGenerate,
    doStream,
  };
  return { doGenerate, doStream, model };
}

async function policyAgent(deliveryPolicy: NotificationDeliveryPolicyConfig) {
  const { doGenerate, doStream, model } = unreachableModel();
  const memory = new MockMemory();
  const agent = new Agent({
    id: 'source-keys',
    name: 'Source keys',
    instructions: 'Never runs.',
    model,
    memory,
    notifications: { deliveryPolicy },
  });
  new Mastra({
    storage: new InMemoryStore(),
    agents: { 'source-keys': agent },
    logger: false,
  });
  const threadId = crypto.randomUUID();
  await memory.saveThread({
    thread: {
      id: threadId,
      resourceId: 'resource-keys',
      createdAt: CREATED_AT,
      updatedAt: CREATED_AT,
      metadata: {},
    },
  });
  const send = async (source: string, priority: NotificationPriority) => {
    const result = await agent.sendNotificationSignal(
      { source, kind: 'changed', summary: 'payload', priority },
      {
        threadId,
        resourceId: 'resource-keys',
        ifIdle: { behavior: 'persist' },
      },
    );
    if (result.persisted) await result.persisted;
    return result;
  };
  const messages = async () => (await memory.recall({ threadId })).messages;
  return { doGenerate, doStream, messages, send };
}

for (const [label, core] of modules) {
  describe.skipIf(!patched.get(label))(
    `core notification summary source keys (${label})`,
    () => {
      it('counts prototype-colliding sources as own numeric entries', () => {
        const summary = core.summarizeNotifications(BATCH);

        // No source name resolves an inherited member instead of its own count. An
        // own `__proto__` entry requires an accumulator that never invokes the
        // inherited setter, which a read-side guard alone cannot produce.
        expect(Object.entries(summary.bySource)).toEqual([
          ['__proto__', 1],
          ['constructor', 1],
          ['toString', 1],
          ['hasOwnProperty', 1],
          ['', 1],
          ['crm', 2],
        ]);
        for (const source of [
          '__proto__',
          'constructor',
          'toString',
          'hasOwnProperty',
          '',
          'crm',
        ]) {
          expect(Object.hasOwn(summary.bySource, source)).toBe(true);
          expect(typeof summary.bySource[source]).toBe('number');
        }
        expect(Object.hasOwn(summary.bySource, 'valueOf')).toBe(false);

        expect(summary.pending).toBe(7);
        expect(summary.threadId).toBe('thread-keys');
        expect(summary.resourceId).toBe('resource-keys');
        expect(summary.agentId).toBe('agent-keys');
        expect(summary.byPriority).toEqual({
          low: 3,
          medium: 2,
          high: 1,
          urgent: 1,
        });
        expect(summary.notificationIds).toEqual([
          'n1',
          'n2',
          'n3',
          'n4',
          'n5',
          'n6',
          'n7',
        ]);
      });

      it('renders own counts in the summary text and metadata groups', () => {
        const summary = core.summarizeNotifications(BATCH);

        expect(core.notificationSummaryContents(summary)).toBe(
          ': 1, __proto__: 1, constructor: 1, crm: 2, hasOwnProperty: 1, toString: 1',
        );
        expect(core.notificationSummarySignalMetadata(summary)).toEqual({
          signal: 'summary',
          pending: 7,
          groups: [
            { source: '', count: 1 },
            { source: '__proto__', count: 1 },
            { source: 'constructor', count: 1 },
            { source: 'crm', count: 2 },
            { source: 'hasOwnProperty', count: 1 },
            { source: 'toString', count: 1 },
          ],
          byPriority: { low: 3, medium: 2, high: 1, urgent: 1 },
          notificationIds: ['n1', 'n2', 'n3', 'n4', 'n5', 'n6', 'n7'],
          priority: 'urgent',
        });
      });

      it('keeps the empty and no-pending shapes', () => {
        const empty = core.summarizeNotifications([]);
        expect(empty).toMatchObject({
          threadId: '',
          pending: 0,
          byPriority: {},
          notificationIds: [],
        });
        expect(Object.entries(empty.bySource)).toEqual([]);
        expect(core.notificationSummaryContents(empty)).toBe(
          'No pending notifications',
        );
        expect(core.notificationSummarySignalMetadata(empty)).toEqual({
          signal: 'summary',
          pending: 0,
          groups: [],
          byPriority: {},
          notificationIds: [],
        });

        const terminal = core.summarizeNotifications([
          record('n9', 'constructor', 'high', 'delivered'),
        ]);
        expect(terminal).toMatchObject({
          threadId: 'thread-keys',
          resourceId: 'resource-keys',
          agentId: 'agent-keys',
          pending: 0,
          byPriority: {},
          notificationIds: [],
        });
        expect(Object.entries(terminal.bySource)).toEqual([]);
        expect(core.notificationSummaryContents(terminal)).toBe(
          'No pending notifications',
        );
      });
    },
  );
}

for (const [label, core] of modules) {
  describe.skipIf(!patched.get(label))(
    `core notification delivery policy source lookup (${label})`,
    () => {
      const input = (
        source: string,
        priority: NotificationPriority = 'low',
      ) => ({
        record: record('p1', source, priority),
        threadState: 'idle' as const,
        now: CREATED_AT,
      });

      it.each(
        POLICY_SOURCES,
      )('falls through an empty source map to the default action for %s', async (source) => {
        await expect(
          core.resolveNotificationDeliveryDecision({
            config: { sources: {}, default: 'discard' },
            ...input(source),
          }),
        ).resolves.toEqual({ action: 'discard' });
      });

      it('honors an explicit own source entry', async () => {
        const config = {
          sources: sourcePolicy([
            ['constructor', 'persist'],
            ['__proto__', 'queue'],
          ]),
          default: 'discard' as const,
        };
        await expect(
          core.resolveNotificationDeliveryDecision({
            config,
            ...input('constructor'),
          }),
        ).resolves.toEqual({ action: 'persist' });
        await expect(
          core.resolveNotificationDeliveryDecision({
            config,
            ...input('__proto__'),
          }),
        ).resolves.toEqual({ action: 'queue' });
        await expect(
          core.resolveNotificationDeliveryDecision({
            config,
            ...input('toString'),
          }),
        ).resolves.toEqual({ action: 'discard' });
      });

      it('keeps decide precedence over the source map', async () => {
        await expect(
          core.resolveNotificationDeliveryDecision({
            config: {
              decide: () => 'deliver',
              sources: sourcePolicy([['constructor', 'persist']]),
              default: 'discard',
            },
            ...input('constructor'),
          }),
        ).resolves.toEqual({ action: 'deliver' });
      });

      it('keeps the priority fallback between the source map and the default', async () => {
        await expect(
          core.resolveNotificationDeliveryDecision({
            config: {
              sources: {},
              priorities: { low: 'queue' },
              default: 'discard',
            },
            ...input('constructor'),
          }),
        ).resolves.toEqual({ action: 'queue' });
      });
    },
  );
}

// Core's inline sender builds its own summary from the helper it imports
// lexically and hands it straight to the thread runtime, so overriding
// `agent.sendSignal` never observes it. It only summarizes while the thread is
// active, which a registered run provides without any model call.
describe.skipIf(!patched.get('esm'))(
  'core inline notification summary sender',
  () => {
    it.each([
      ['medium', 'constructor', 'active-batch-summary'],
      ['high', '__proto__', 'active-high-summary-then-full'],
    ] as const)('emits an own-keyed inline summary for an active %s notification', async (priority, source, reason) => {
      // #given — an ordinary core agent with a run in flight
      const { doGenerate, doStream, model } = unreachableModel();
      const memory = new MockMemory();
      const agent = new Agent({
        id: 'inline-summary',
        name: 'Inline summary',
        instructions: 'Never runs.',
        model,
        memory,
      });
      const mastra = new Mastra({
        storage: new InMemoryStore(),
        agents: { 'inline-summary': agent },
        logger: false,
      });
      const runtime = mastra.agentThreadStreamRuntime;
      const pubsub = agent.getPubSub();
      const threadId = crypto.randomUUID();
      await memory.saveThread({
        thread: {
          id: threadId,
          resourceId: 'resource-keys',
          createdAt: CREATED_AT,
          updatedAt: CREATED_AT,
          metadata: {},
        },
      });
      await agent.stream('active turn', {
        runId: crypto.randomUUID(),
        memory: { thread: threadId, resource: 'resource-keys' },
      });
      await vi.waitFor(() =>
        expect(
          runtime.getThreadState(
            { threadId, resourceId: 'resource-keys' },
            pubsub,
          ),
        ).toBe('active'),
      );
      const emitted = vi.spyOn(runtime, 'sendSignal');

      try {
        // #when
        const result = await agent.sendNotificationSignal(
          { source, kind: 'changed', summary: 'payload', priority },
          {
            threadId,
            resourceId: 'resource-keys',
            ifIdle: { behavior: 'persist' },
          },
        );

        // #then — the emitted summary counts the colliding source once
        expect(result.decision).toMatchObject({ action: 'summarize', reason });
        const summaries = emitted.mock.calls
          .map(([, signal]) => signal)
          .filter((signal) => signal.tagName === 'notification-summary');
        expect(summaries.map((signal) => signal.contents)).toEqual([
          `${source}: 1`,
        ]);
        expect(
          (summaries[0]?.metadata as { notification?: unknown } | undefined)
            ?.notification,
        ).toEqual({
          signal: 'summary',
          pending: 1,
          groups: [{ source, count: 1 }],
          byPriority: { [priority]: 1 },
          notificationIds: [result.record.id],
          priority,
        });
        // A summarized high notification keeps its full-delivery cursor; a medium
        // one has none to keep.
        const { deliverAt } = result.decision as { deliverAt?: Date };
        if (priority === 'high') expect(deliverAt).toBeInstanceOf(Date);
        else expect(deliverAt).toBeUndefined();
        expect(doStream).not.toHaveBeenCalled();
        expect(doGenerate).not.toHaveBeenCalled();
      } finally {
        runtime.abortThread({ threadId, resourceId: 'resource-keys' }, pubsub);
      }
      expect(unhandled).toEqual([]);
    });
  },
);

describe.skipIf(!patched.get('esm'))(
  'ordinary core agent delivery policy',
  () => {
    it.each([
      'ordinary',
      'constructor',
      'toString',
      '__proto__',
    ] as const)('discards %s under an empty source map with a discard default', async (source) => {
      const agent = await policyAgent({ sources: {}, default: 'discard' });

      const result = await agent.send(source, 'low');

      expect(result.record.status).toBe('discarded');
      expect(result.decision).toMatchObject({ action: 'discard' });
      expect(await agent.messages()).toHaveLength(0);
      expect(agent.doGenerate).not.toHaveBeenCalled();
      expect(agent.doStream).not.toHaveBeenCalled();
    });

    it('honors an explicit own source entry through the sender', async () => {
      const agent = await policyAgent({
        sources: sourcePolicy([['constructor', 'persist']]),
        default: 'discard',
      });

      const result = await agent.send('constructor', 'low');

      expect(result.record.status).toBe('pending');
      expect(result.decision).toMatchObject({ action: 'persist' });
      expect(agent.doGenerate).not.toHaveBeenCalled();
      expect(agent.doStream).not.toHaveBeenCalled();
    });

    it('keeps decide precedence and the priority fallback through the sender', async () => {
      const decided = await policyAgent({
        decide: () => 'queue',
        sources: {},
        default: 'discard',
      });
      const decidedResult = await decided.send('constructor', 'low');
      expect(decidedResult.decision).toMatchObject({ action: 'queue' });
      expect(decided.doGenerate).not.toHaveBeenCalled();

      const prioritized = await policyAgent({
        sources: {},
        priorities: { low: 'queue' },
        default: 'discard',
      });
      const prioritizedResult = await prioritized.send('constructor', 'low');
      expect(prioritizedResult.decision).toMatchObject({ action: 'queue' });
      expect(prioritized.doGenerate).not.toHaveBeenCalled();
    });
  },
);
