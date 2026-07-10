/**
 * Content Pipeline Workflow -- Design Sketch
 *
 * Mastra createWorkflow() example. Illustrates parallel step execution.
 * Not runnable as-is.
 */

import { createStep, createWorkflow } from '@mastra/core/workflows';
import { z } from 'zod';

const researchTopic = createStep({
  id: 'researchTopic',
  inputSchema: z.object({ topic: z.string() }),
  outputSchema: z.object({
    outline: z.array(z.string()),
    sources: z.array(z.string()),
  }),
  execute: async ({ inputData }) => ({ outline: [], sources: [] }),
});

// Parallel section writers -- each receives researchTopic's output.
const sectionInput = z.object({
  outline: z.array(z.string()),
  sources: z.array(z.string()),
});
const sectionOutput = z.object({ content: z.string() });

const writeIntro = createStep({
  id: 'writeIntro',
  inputSchema: sectionInput,
  outputSchema: sectionOutput,
  execute: async ({ inputData }) => ({ content: '' }),
});

const writeBody = createStep({
  id: 'writeBody',
  inputSchema: sectionInput,
  outputSchema: sectionOutput,
  execute: async ({ inputData }) => ({ content: '' }),
});

const writeConclusion = createStep({
  id: 'writeConclusion',
  inputSchema: sectionInput,
  outputSchema: sectionOutput,
  execute: async ({ inputData }) => ({ content: '' }),
});

// .parallel() output is keyed by step id.
const reviewContent = createStep({
  id: 'reviewContent',
  inputSchema: z.object({
    writeIntro: sectionOutput,
    writeBody: sectionOutput,
    writeConclusion: sectionOutput,
  }),
  outputSchema: z.object({ edits: z.array(z.any()), approved: z.boolean() }),
  execute: async ({ inputData }) => ({ edits: [], approved: false }),
});

export const contentPipeline = createWorkflow({
  id: 'content-pipeline',
  inputSchema: z.object({ topic: z.string() }),
  outputSchema: z.object({ edits: z.array(z.any()), approved: z.boolean() }),
})
  .then(researchTopic)
  .parallel([writeIntro, writeBody, writeConclusion])
  .then(reviewContent)
  .commit();
