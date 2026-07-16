// SPDX-License-Identifier: Apache-2.0
// The recall-path proof (docs/agent-memory-tenancy.md item 6) — the obligation
// the memory chokepoints were built for, discharged at the level an agent
// actually reads memory rather than at the SQL level.
//
// Why AGENT-level and not the storage-level pin mastra-schema-guard.test.ts
// already carries: that one proves the ROWS are disjoint. This proves the thing
// that would actually leak — that `MastraMemory.recall()`, the seam an agent
// calls to load context before a turn, cannot surface another tenant's messages
// even when both tenants name the SAME business key. Rows being disjoint is
// worthless if the recall API scopes by something else; nothing but this test
// says it does not.
//
// The adversarial fixture is the whole point: `resourceId = 'user-1'` for BOTH
// tenants. Hosts derive resourceIds from user identity (a user id, an email), so
// the collision is the EXPECTED case, not a contrived one — unsalted, both
// tenants' agents would read one conversation.
//
// MockMemory is core's own MastraMemory implementation (the recall/listThreads/
// getWorkingMemory bodies below are core's, not ours) driven over the REAL
// @mastra/cloudflare-d1 D1Store: the proof rides production's storage adapter,
// not an in-memory stand-in, and adds no dependency for a test.

import { MockMemory } from '@mastra/core/memory';
import type { InMemoryStore, MastraCompositeStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import { createD1Storage } from './d1-storage.js';
import { init } from './init.js';
import { mintResourceId, mintThreadId } from './memory-id.js';

/** The business key BOTH tenants use — the collision the salting exists for. */
const SHARED_BUSINESS_KEY = 'user-1';

function memoryOver(storage: MastraCompositeStore): MockMemory {
  // MockMemory's ctor types `storage` as InMemoryStore, but its body only ever
  // calls storage.getStore('memory') — the D1Store satisfies that, so the cast
  // buys the real adapter under core's own memory implementation.
  return new MockMemory({
    storage: storage as unknown as InMemoryStore,
    enableWorkingMemory: true,
  });
}

/** A tenant's memory, keyed the way a host would: minted from its own tenant. */
async function seedTenant(memory: MockMemory, tenantId: string) {
  const threadId = mintThreadId(tenantId, () => 'thread-1');
  const resourceId = mintResourceId(tenantId, SHARED_BUSINESS_KEY);
  const now = new Date();
  await memory.saveThread({
    thread: {
      id: threadId,
      resourceId,
      title: `${tenantId} conversation`,
      createdAt: now,
      updatedAt: now,
    },
  });
  await memory.saveMessages({
    messages: [
      {
        id: `${tenantId}-m1`,
        threadId,
        resourceId,
        role: 'user',
        createdAt: now,
        content: {
          format: 2,
          parts: [{ type: 'text', text: `${tenantId} private message` }],
        },
      },
    ],
  });
  await memory.updateWorkingMemory({
    threadId,
    resourceId,
    workingMemory: `${tenantId} working memory`,
  });
  return { threadId, resourceId };
}

/** The real D1 adapter, with its tables created the way production creates them. */
async function d1Memory(): Promise<MockMemory> {
  const storage = createD1Storage({
    binding: d1DatabaseLike(openSqlite()) as never,
  });
  // Mastra creates the six tables eagerly on the first persistence op; the
  // node:sqlite harness cannot run the memory domain's own lazy DDL, so a
  // throwaway run is what stands the schema up (same idiom as the schema guard).
  const { createWorkflow, createStep, runtime } = init({ storage });
  const noop = createStep({
    id: 'noop',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => ({}),
  });
  createWorkflow({
    id: 'seed',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  })
    .then(noop)
    .commit();
  await runtime.start('seed', { runId: 'seed_r0', inputData: {} });
  return memoryOver(storage);
}

function textOf(messages: Array<{ content?: unknown }>): string[] {
  return messages.flatMap((message) => {
    const parts = (message.content as { parts?: Array<{ text?: string }> })
      ?.parts;
    return (parts ?? []).map((part) => part.text ?? '');
  });
}

describe('agent-memory recall is tenant-disjoint for the SAME business key', () => {
  it("tenant B's recall never returns tenant A's messages", async () => {
    // #given — two tenants whose agents both key memory by 'user-1'
    const memory = await d1Memory();
    const acme = await seedTenant(memory, 'acme');
    const globex = await seedTenant(memory, 'globex');
    // The shared key produced DISJOINT ids — the leak is closed BEFORE any read
    expect(acme.resourceId).toBe('acme_user-1');
    expect(globex.resourceId).toBe('globex_user-1');

    // #when — tenant B's agent loads its context, exactly as a turn would
    const recalled = await memory.recall({
      threadId: globex.threadId,
      resourceId: globex.resourceId,
    });

    // #then — its own message, and NOTHING of tenant A's
    expect(textOf(recalled.messages)).toEqual(['globex private message']);
  });

  it("tenant B's thread discovery over the shared key never lists tenant A's threads", async () => {
    // #given — the resourceId lookup a host runs to find "this user's"
    // conversations. Unsalted, both tenants' threads would come back here.
    const memory = await d1Memory();
    const acme = await seedTenant(memory, 'acme');
    const globex = await seedTenant(memory, 'globex');

    // #when
    const listed = await memory.listThreads({
      filter: { resourceId: globex.resourceId },
    });

    // #then
    expect(listed.threads.map((thread) => thread.id)).toEqual([
      globex.threadId,
    ]);
    expect(listed.threads.map((thread) => thread.id)).not.toContain(
      acme.threadId,
    );
  });

  it("tenant B's working memory over the shared key is never tenant A's", async () => {
    // #given — working memory is RESOURCE-scoped, so it keys on the business
    // key most directly of all three recall paths
    const memory = await d1Memory();
    await seedTenant(memory, 'acme');
    const globex = await seedTenant(memory, 'globex');

    // #when
    const working = await memory.getWorkingMemory({
      threadId: globex.threadId,
      resourceId: globex.resourceId,
    });

    // #then
    expect(working).toBe('globex working memory');
  });

  it('a foreign threadId + own resourceId recalls nothing — but this is INCIDENTAL, not a second line of defense', async () => {
    // #given — a paired mismatch: tenant A's thread, tenant B's resource.
    const memory = await d1Memory();
    const acme = await seedTenant(memory, 'acme');
    const globex = await seedTenant(memory, 'globex');

    // #when
    const recalled = await memory.recall({
      threadId: acme.threadId,
      resourceId: globex.resourceId,
    });

    // #then — the resourceId predicate excludes A's messages.
    expect(recalled.messages).toEqual([]);

    // ...but do NOT read this as defense in depth. The adapter applies the
    // resourceId filter CONDITIONALLY (`if (resourceId) query += ' AND
    // resourceId = ?'`), and core itself ships recall call sites that pass a
    // threadId alone — so the same foreign thread recalls in FULL when the
    // caller omits the resource, as the next assertion pins. The ONLY thing
    // standing between a foreign threadId and its history is the host boundary
    // (requireOwnedMemoryId); nothing downstream re-checks it.
    const unscoped = await memory.recall({ threadId: acme.threadId });
    expect(textOf(unscoped.messages)).toEqual(['acme private message']);
  });
});
