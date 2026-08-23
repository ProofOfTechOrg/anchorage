// SPDX-License-Identifier: Apache-2.0

import { approvalGrantProvider } from '@proofoftech/flowsafe/approval-api';
import {
  ExecutionFenceStore,
  init,
  type RunnerRuntime,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  assertWorkflowsRegistered,
  type WorkflowMeta,
} from '@proofoftech/flowsafe/host-kit';
import { z } from 'zod';

import { createComposedStorage } from './storage.js';

export const WORKFLOWS: ReadonlyArray<WorkflowMeta> = [
  {
    id: 'starter-echo',
    title: 'Starter echo',
    description:
      'A minimal durable workflow beside the runtime-driven agent surface.',
    sampleInput: { message: 'hello' },
  },
];

export function defineWorkflows(env: Env): RunnerRuntime {
  const { createStep, createWorkflow, runtime } = init(
    { storage: createComposedStorage(env.DB) },
    {
      requestContextForRun: approvalGrantProvider(
        approvalStoreFactoryFor(env.DB).store(),
      ),
      // The composed store hides the binding init would have fenced from, so
      // the run object names it: the deployment execution fence must live in
      // the SAME database as the state it fences.
      executionFence: new ExecutionFenceStore(env.DB),
    },
  );
  const schema = z.object({ message: z.string().min(1).max(5_000) });
  const echo = createStep({
    id: 'echo',
    inputSchema: schema,
    outputSchema: schema,
    execute: async ({ inputData }) => inputData,
  });
  createWorkflow({
    id: 'starter-echo',
    inputSchema: schema,
    outputSchema: schema,
  })
    .then(echo)
    .commit();
  assertWorkflowsRegistered(runtime, WORKFLOWS);
  return runtime;
}
