// Module 5/5 — Access Request: request access to a resource, grant it after
// approval. Capabilities shown: route-level RBAC (meta.allowedRoles restricts
// START to admin/builder — enforced by the host route), cross-workflow
// isolation on the grant connector (a request naming another workflow's scope is
// denied), and separation of duties (the requester cannot approve their own
// request — enforced by ApprovalService). In-step actor minting is intentionally
// out of scope: the runtime does not track the starting actor, so route-level
// RBAC (meta.allowedRoles) is the demonstrated control here.

import {
  createConnector,
  crossWorkflowIsolation,
} from '@proofoftech/breakwater';
import { z } from 'zod';

import type { WorkflowModule } from '../../src/host-kit/index.js';
import {
  callConnector,
  type ShowcaseModuleDeps,
  showcaseResumeSchema,
} from './shared.js';

export const ACCESS_CONNECTOR = 'grant-access';
const WORKFLOW_ID = 'access-request';

export const accessRequestModule: WorkflowModule<ShowcaseModuleDeps> = {
  meta: {
    id: WORKFLOW_ID,
    title: 'Access Request',
    description:
      'Request access to a resource and grant it after approval. Start is restricted to admin/builder (route RBAC); the grant connector enforces cross-workflow isolation, and the requester cannot self-approve (SoD).',
    sampleInput: {
      resource: 'prod-database',
      role: 'reader',
      justification: 'on-call debugging',
    },
    allowedRoles: ['admin', 'builder'],
  },
  register({ createWorkflow, createStep, audit }) {
    // The gated write. crossWorkflowIsolation denies granting access to a
    // resource scoped to a DIFFERENT workflow: targetScopeOf reads the request's
    // optional `targetScope`; if it names a scope other than this workflow's
    // (the runtime-minted 'access-request'), the evaluator fails closed.
    const grantAccessConnector = createConnector<
      { resource: string; role: string; targetScope?: string },
      { granted: boolean; resource: string; role: string }
    >({
      id: ACCESS_CONNECTOR,
      description: 'Grants the requested access after approval',
      inputSchema: z.object({
        resource: z.string(),
        role: z.string(),
        targetScope: z.string().optional(),
      }),
      outputSchema: z.object({
        granted: z.boolean(),
        resource: z.string(),
        role: z.string(),
      }),
      permissions: { sideEffect: 'write', requiresApproval: true },
      policies: {
        audit,
        evaluators: [
          crossWorkflowIsolation({
            targetScopeOf: (call) =>
              (call.input as { targetScope?: string }).targetScope,
          }),
        ],
      },
      execute: async ({ resource, role }) => {
        console.log(JSON.stringify({ type: 'access-granted', resource, role }));
        return { granted: true, resource, role };
      },
    });

    const requestAccess = createStep({
      id: 'requestAccess',
      inputSchema: z.object({
        resource: z.string(),
        role: z.string(),
        justification: z.string(),
        targetScope: z.string().optional(),
      }),
      outputSchema: z.object({
        resource: z.string(),
        role: z.string(),
        justification: z.string(),
        targetScope: z.string().optional(),
      }),
      execute: async ({ inputData }) => ({
        resource: inputData.resource.trim(),
        role: inputData.role.trim().toLowerCase(),
        justification: inputData.justification.trim(),
        targetScope: inputData.targetScope,
      }),
    });

    const approveAccess = createStep({
      id: 'approveAccess',
      inputSchema: z.object({
        resource: z.string(),
        role: z.string(),
        justification: z.string(),
        targetScope: z.string().optional(),
      }),
      outputSchema: z.object({
        resource: z.string(),
        role: z.string(),
        targetScope: z.string().optional(),
        approved: z.boolean(),
        decidedBy: z.string().optional(),
      }),
      suspendSchema: z.object({
        reason: z.string(),
        connectors: z.array(z.string()),
        resource: z.string(),
        role: z.string(),
      }),
      resumeSchema: showcaseResumeSchema,
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          return suspend({
            reason: `approve ${inputData.role} access to ${inputData.resource}`,
            connectors: [ACCESS_CONNECTOR],
            resource: inputData.resource,
            role: inputData.role,
          });
        }
        return {
          resource: inputData.resource,
          role: inputData.role,
          targetScope: inputData.targetScope,
          approved: resumeData.approved,
          decidedBy: resumeData.decidedBy,
        };
      },
    });

    const grantAccess = createStep({
      id: 'grantAccess',
      inputSchema: z.object({
        resource: z.string(),
        role: z.string(),
        targetScope: z.string().optional(),
        approved: z.boolean(),
        decidedBy: z.string().optional(),
      }),
      outputSchema: z.object({
        granted: z.boolean(),
        resource: z.string(),
        role: z.string(),
      }),
      execute: async ({ inputData, requestContext }) => {
        if (!inputData.approved) {
          return {
            granted: false,
            resource: inputData.resource,
            role: inputData.role,
          };
        }
        return callConnector<
          { resource: string; role: string; targetScope?: string },
          { granted: boolean; resource: string; role: string }
        >(
          grantAccessConnector,
          {
            resource: inputData.resource,
            role: inputData.role,
            targetScope: inputData.targetScope,
          },
          requestContext,
        );
      },
    });

    createWorkflow({
      id: WORKFLOW_ID,
      inputSchema: z.object({
        resource: z.string(),
        role: z.string(),
        justification: z.string(),
        targetScope: z.string().optional(),
      }),
      outputSchema: z.object({
        granted: z.boolean(),
        resource: z.string(),
        role: z.string(),
      }),
    })
      .then(requestAccess)
      .then(approveAccess)
      .then(grantAccess)
      .commit();
  },
};
