// SPDX-License-Identifier: Apache-2.0

import type { MastraModelConfig } from '@mastra/core/llm';
import {
  type AuditLogger,
  createConnector,
  createGuardedAgent,
  D1RateLimitStore,
  type RateLimitDatabase,
  tenantIsolation,
} from '@proofoftech/breakwater';
import type { AgentMeta, AgentModule } from '@proofoftech/flowsafe/agent-host';
import { z } from 'zod';

export const STARTER_AGENT_ID = 'anchorage-agent';
export const RECORD_ACTION_CONNECTOR_ID = 'starter_recordAction';
export const STARTER_AGENT_META = {
  id: STARTER_AGENT_ID,
  title: 'Anchorage durable agent',
  description:
    'Records one approval-gated operation in the tenant-isolated starter ledger.',
  allowedRoles: ['admin', 'operator', 'builder'],
  // Every automated entry this starter actually wires, and nothing else.
  // Naming entry paths rather than just kinds is what stops a scheduler from
  // also arriving through a signal. 'approval.resume' is deliberately absent:
  // resuming is implied by the kind that started the run, so an automated run
  // that suspends for approval is not stranded once a human approves it.
  //
  // worker.ts wires all three: scheduleTick.startAgent (system/schedule.fire),
  // createNotificationDispatchTick (system/notification.dispatch), and the
  // signal-provider host whose deliveries arrive as a service principal.
  allowedAutomation: [
    {
      kind: 'system',
      entryPaths: ['schedule.fire', 'notification.dispatch'],
    },
    { kind: 'service', entryPaths: ['signal.notification'] },
  ],
} as const satisfies AgentMeta;

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

export function createStarterAgentModule(options: {
  model: MastraModelConfig;
  db: Env['DB'];
  audit: AuditLogger;
}): AgentModule {
  const recordAction = createRecordActionConnector(options.db);
  const agent = createGuardedAgent({
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
    allowedRoles: STARTER_AGENT_META.allowedRoles,
    // Must mirror STARTER_AGENT_META.allowedAutomation's kinds; the catalog
    // refuses the module at construction if the two ever drift.
    allowedPrincipalKinds: ['human', 'system', 'service'],
    policies: [],
    audit: options.audit,
    maxSteps: 1,
    toolChoice: 'required',
  });
  return {
    meta: STARTER_AGENT_META,
    agent,
  };
}
