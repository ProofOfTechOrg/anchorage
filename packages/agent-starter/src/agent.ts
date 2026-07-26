// SPDX-License-Identifier: Apache-2.0

import { Agent } from '@mastra/core/agent';
import type { MastraModelConfig } from '@mastra/core/llm';
import {
  createConnector,
  D1RateLimitStore,
  type RateLimitDatabase,
  tenantIsolation,
} from '@proofoftech/breakwater';
import { z } from 'zod';

export const STARTER_AGENT_ID = 'anchorage-agent';
export const RECORD_ACTION_CONNECTOR_ID = 'starter_recordAction';

const actionInput = z.object({
  action: z.string().min(1).max(500),
});
const actionOutput = z.object({
  actionId: z.string(),
  recorded: z.boolean(),
});

function rateLimitDatabase(db: Env['DB']): RateLimitDatabase {
  return db as unknown as RateLimitDatabase;
}

export function createRecordActionConnector(db: Env['DB']) {
  return createConnector({
    id: RECORD_ACTION_CONNECTOR_ID,
    description:
      'Record one approved action in the tenant-isolated starter ledger',
    inputSchema: actionInput,
    outputSchema: actionOutput,
    execute: async ({ action }, context) => {
      const toolCallId = context.agent?.toolCallId;
      const threadId = context.agent?.threadId;
      const resourceId = context.agent?.resourceId;
      if (!toolCallId || !threadId || !resourceId) {
        throw new Error(
          'the write connector requires a durable tool-call and memory binding',
        );
      }
      await db
        .prepare(
          `CREATE TABLE IF NOT EXISTS starter_actions (
             action_id TEXT PRIMARY KEY,
             thread_id TEXT NOT NULL,
             resource_id TEXT NOT NULL,
             action TEXT NOT NULL,
             created_at TEXT NOT NULL
           )`,
        )
        .run();
      const result = await db
        .prepare(
          `INSERT OR IGNORE INTO starter_actions
           (action_id, thread_id, resource_id, action, created_at)
           VALUES (?, ?, ?, ?, ?)`,
        )
        .bind(
          toolCallId,
          threadId,
          resourceId,
          action,
          new Date().toISOString(),
        )
        .run();
      return {
        actionId: toolCallId,
        recorded: (result.meta?.changes ?? 0) > 0,
      };
    },
    permissions: {
      sideEffect: 'write',
      requiresApproval: true,
      rateLimit: '10/min',
    },
    policies: {
      writePermissions: {
        requireApproval: [RECORD_ACTION_CONNECTOR_ID],
      },
      evaluators: [tenantIsolation()],
      rateLimitStore: new D1RateLimitStore(rateLimitDatabase(db)),
    },
  });
}

export function createStarterAgent(options: {
  model: MastraModelConfig;
  db: Env['DB'];
}) {
  const recordAction = createRecordActionConnector(options.db);
  return new Agent({
    id: STARTER_AGENT_ID,
    name: 'Anchorage durable agent',
    instructions: [
      'You are an approval-gated operations agent.',
      `For each user request, call ${RECORD_ACTION_CONNECTOR_ID} exactly once with a concise description of the requested action.`,
      'Do not claim the action happened until the tool returns.',
    ].join(' '),
    model: options.model,
    tools: {
      [RECORD_ACTION_CONNECTOR_ID]: recordAction,
    },
  });
}
