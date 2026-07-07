/**
 * Custom Workflow Scoping -- Design Sketch
 *
 * Demonstrates breakwater RBAC scoping on a Mastra workflow.
 * Not runnable as-is.
 */

import { createWorkflow, createStep } from '@mastra/core/workflows';
import { z } from 'zod';
import { RBACMiddleware } from '@proofoftech/breakwater/rbac';

const sensitiveOperation = createStep({
  id: 'sensitiveOperation',
  inputSchema: z.object({}),
  outputSchema: z.object({ result: z.string() }),
  // Guarded by RBAC -- only builder and admin roles
  execute: async ({ inputData }) => ({ result: 'done' }),
});

export const scopedWorkflow = createWorkflow({
  id: 'custom-workflow-scoping',
  inputSchema: z.object({}),
  outputSchema: z.object({ result: z.string() }),
})
  .then(sensitiveOperation)
  .commit();

// scopedWorkflow is wrapped with RBAC scope at deployment time:
// Scope: builder, admin
// Per-workflow namespace prevents cross-workflow access
