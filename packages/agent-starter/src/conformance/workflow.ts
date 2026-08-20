// SPDX-License-Identifier: Apache-2.0

import type { D1Database } from '@cloudflare/workers-types';
import { createConnector, invokeConnector } from '@proofoftech/breakwater';
import { approvalGrantProvider } from '@proofoftech/flowsafe/approval-api';
import { init, type RunnerRuntime } from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  assertWorkflowsRegistered,
  type WorkflowMeta,
} from '@proofoftech/flowsafe/host-kit';
import { z } from 'zod';

import { CONFORMANCE_WORKFLOW_ID } from './contract.js';
import type { ConformanceStateEnv } from './env.js';

export const RECORD_EFFECT_CONNECTOR_ID = 'conformance_recordEffect';

export const CONFORMANCE_WORKFLOWS: ReadonlyArray<WorkflowMeta> = [
  {
    id: CONFORMANCE_WORKFLOW_ID,
    title: 'Conformance approval',
    description:
      'Suspends for one approval, then performs exactly one durable effect.',
    sampleInput: { effectNonce: '00000000-0000-4000-8000-000000000000' },
  },
];

const EFFECTS_DDL = `CREATE TABLE IF NOT EXISTS conformance_effects (
  effect_nonce TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  created_at TEXT NOT NULL
)`;

/**
 * The gate reports this after approval and after both replay attempts, so a
 * second execution is visible as a count of 2 rather than as a silent success.
 * Creating the table here as well as in the connector keeps a pre-effect read
 * at zero instead of failing on a table that does not exist yet.
 */
export async function countEffects(
  db: D1Database,
  effectNonce: string,
): Promise<number> {
  await db.prepare(EFFECTS_DDL).run();
  const row = await db
    .prepare(
      'SELECT COUNT(*) AS effects FROM conformance_effects WHERE effect_nonce = ?',
    )
    .bind(effectNonce)
    .first<{ effects: number }>();
  return row?.effects ?? 0;
}

const workflowInput = z.object({ effectNonce: z.string().min(1).max(200) });
const effectOutput = z.object({
  effectNonce: z.string(),
  recorded: z.boolean(),
});

function createRecordEffectConnector(db: D1Database) {
  return createConnector<
    { effectNonce: string; runId: string },
    { effectNonce: string; recorded: boolean }
  >({
    id: RECORD_EFFECT_CONNECTOR_ID,
    description: 'Record the one approved conformance effect',
    inputSchema: z.object({
      effectNonce: z.string().min(1).max(200),
      runId: z.string().min(1).max(200),
    }),
    outputSchema: effectOutput,
    // Grant-only, exactly as the workerd spike proves it: the write gate is
    // satisfied by a requestContext grant the runtime derives from an APPROVED
    // record, never by anything in the resume body.
    permissions: { sideEffect: 'write', requiresApproval: true },
    execute: async ({ effectNonce, runId }) => {
      await db.prepare(EFFECTS_DDL).run();
      const result = await db
        .prepare(
          `INSERT OR IGNORE INTO conformance_effects (effect_nonce, run_id, created_at)
           VALUES (?, ?, ?)`,
        )
        .bind(effectNonce, runId, new Date().toISOString())
        .run();
      return { effectNonce, recorded: (result.meta?.changes ?? 0) > 0 };
    },
  });
}

/**
 * approval -> effect. Two steps rather than the spike's three: the gate reads
 * `effectCount`, not a research artefact, so a preparatory step would add a
 * D1 write without adding evidence.
 */
export function defineConformanceWorkflows(
  env: ConformanceStateEnv,
): RunnerRuntime {
  const approvals = approvalStoreFactoryFor(env.DB).store();
  const { createStep, createWorkflow, runtime } = init(env, {
    requestContextForRun: approvalGrantProvider(approvals),
  });
  const recorder = createRecordEffectConnector(env.DB);

  const approval = createStep({
    id: 'approval',
    inputSchema: workflowInput,
    outputSchema: workflowInput.extend({ approved: z.boolean() }),
    // `connectors` is the server-authored literal every host-kit bridge reads
    // to decide what an approval mints. It must never come from run input.
    suspendSchema: z.object({
      reason: z.string(),
      connectors: z.array(z.string()),
    }),
    resumeSchema: z.object({
      approved: z.boolean(),
      comment: z.string().optional(),
      decidedBy: z.string().optional(),
    }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return suspend({
          reason: 'conformance approval required before the durable effect',
          connectors: [RECORD_EFFECT_CONNECTOR_ID],
        });
      }
      return { ...inputData, approved: resumeData.approved };
    },
  });

  const effect = createStep({
    id: 'effect',
    inputSchema: workflowInput.extend({ approved: z.boolean() }),
    outputSchema: effectOutput,
    execute: async ({ inputData, requestContext, runId }) => {
      if (!inputData.approved) {
        return { effectNonce: inputData.effectNonce, recorded: false };
      }
      return invokeConnector(
        recorder,
        { effectNonce: inputData.effectNonce, runId },
        { requestContext },
      );
    },
  });

  createWorkflow({
    id: CONFORMANCE_WORKFLOW_ID,
    inputSchema: workflowInput,
    outputSchema: effectOutput,
  })
    .then(approval)
    .then(effect)
    .commit();
  assertWorkflowsRegistered(runtime, CONFORMANCE_WORKFLOWS);
  return runtime;
}
