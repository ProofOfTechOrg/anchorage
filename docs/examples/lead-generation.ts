/**
 * Lead Generation Workflow -- Design Sketch
 *
 * Mastra createWorkflow() example with conditional branching and
 * approval gates. Not runnable as-is.
 */

import { createStep, createWorkflow } from '@mastra/core/workflows';
import { z } from 'zod';

const scoredOutput = z.object({
  scored: z.array(z.any()),
  hot: z.array(z.any()),
  cold: z.array(z.any()),
});

const scoreLeads = createStep({
  id: 'scoreLeads',
  inputSchema: z.object({ leads: z.array(z.any()) }),
  outputSchema: scoredOutput,
  execute: async ({ inputData }) => ({ scored: [], hot: [], cold: [] }),
});

const fastTrack = createStep({
  id: 'fastTrack',
  inputSchema: scoredOutput,
  outputSchema: z.object({ assigned: z.boolean() }),
  execute: async ({ inputData }) => ({ assigned: true }),
});

const nurture = createStep({
  id: 'nurture',
  inputSchema: scoredOutput,
  outputSchema: z.object({ queued: z.boolean() }),
  execute: async ({ inputData }) => ({ queued: true }),
});

// .branch() output is keyed by the matching step id.
const reviewHotLeads = createStep({
  id: 'reviewHotLeads',
  inputSchema: z.object({
    fastTrack: z.object({ assigned: z.boolean() }).optional(),
    nurture: z.object({ queued: z.boolean() }).optional(),
  }),
  outputSchema: z.object({ approved: z.boolean() }),
  // Human approval via flowsafe
  execute: async ({ inputData }) => ({ approved: true }),
});

export const leadGeneration = createWorkflow({
  id: 'lead-generation',
  inputSchema: z.object({ leads: z.array(z.any()) }),
  outputSchema: z.object({ approved: z.boolean() }),
})
  .then(scoreLeads)
  .branch([
    [async ({ inputData }) => inputData.hot.length > 0, fastTrack],
    [async ({ inputData }) => inputData.cold.length > 0, nurture],
  ])
  .then(reviewHotLeads)
  .commit();
