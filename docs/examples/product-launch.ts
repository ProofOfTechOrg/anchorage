/**
 * Product Launch Workflow -- Design Sketch
 *
 * Mastra createWorkflow() example with serial pipeline and
 * approval checkpoints. Not runnable as-is.
 */

import { createWorkflow, createStep } from '@mastra/core/workflows';
import { z } from 'zod';

const validateReadiness = createStep({
  id: 'validateReadiness',
  inputSchema: z.object({ productName: z.string() }),
  outputSchema: z.object({ checks: z.array(z.string()), passed: z.boolean() }),
  execute: async ({ inputData }) => ({ checks: [], passed: false }),
});

const approveLaunch = createStep({
  id: 'approveLaunch',
  inputSchema: z.object({ checks: z.array(z.string()), passed: z.boolean() }),
  outputSchema: z.object({ approved: z.boolean(), reviewer: z.string().nullable() }),
  // Human approval via flowsafe dashboard
  execute: async ({ inputData }) => ({ approved: false, reviewer: null }),
});

const executeLaunch = createStep({
  id: 'executeLaunch',
  inputSchema: z.object({ approved: z.boolean(), reviewer: z.string().nullable() }),
  outputSchema: z.object({ deployed: z.boolean(), url: z.string() }),
  execute: async ({ inputData }) => ({ deployed: true, url: '' }),
});

const postLaunchMonitoring = createStep({
  id: 'postLaunchMonitoring',
  inputSchema: z.object({ deployed: z.boolean(), url: z.string() }),
  outputSchema: z.object({ healthy: z.boolean(), metrics: z.record(z.string(), z.any()) }),
  execute: async ({ inputData }) => ({ healthy: true, metrics: {} }),
});

export const productLaunch = createWorkflow({
  id: 'product-launch',
  inputSchema: z.object({ productName: z.string() }),
  outputSchema: z.object({ healthy: z.boolean(), metrics: z.record(z.string(), z.any()) }),
})
  .then(validateReadiness)
  .then(approveLaunch)
  .then(executeLaunch)
  .then(postLaunchMonitoring)
  .commit();
