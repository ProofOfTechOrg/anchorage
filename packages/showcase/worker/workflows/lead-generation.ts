// Module 3/5 — Lead Generation: score leads, route hot/cold via .branch(), then
// assign the hot ones after review. Capabilities shown: conditional branching
// with a post-fan-in gate, a network-egress-allowlisted + rate-limited CRM write
// connector (binding-gated; simulated offline).

import { createConnector, tenantIsolation } from '@proofoftech/breakwater';
import type { WorkflowModule } from '@proofoftech/flowsafe/host-kit/module';
import { z } from 'zod';
import {
  callConnector,
  type ShowcaseModuleDeps,
  showcaseResumeSchema,
} from '#worker/workflows/shared';

export const CRM_ASSIGN_CONNECTOR = 'crm-assign';
/** The one host the assign connector is allowed to reach (egress allowlist). */
const CRM_HOST = 'crm.example.com';
const WORKFLOW_ID = 'lead-generation';

const leadSchema = z.object({
  name: z.string(),
  title: z.string(),
  company: z.string(),
  companySize: z.number(),
});
type Lead = z.infer<typeof leadSchema>;

const assignmentSchema = z.object({ name: z.string(), company: z.string() });

// A lead is "hot" if it is a senior buyer or at a large company — a real (if
// simple) heuristic, not a coin flip, so the branch routing is meaningful.
const SENIOR_TITLE = /\b(VP|Director|Head|Chief|C[A-Z]O)\b/i;
function isHot(lead: Lead): boolean {
  return SENIOR_TITLE.test(lead.title) || lead.companySize >= 200;
}

const scoredOutput = z.object({
  hot: z.array(leadSchema),
  cold: z.array(leadSchema),
});
const fastTrackOutput = z.object({
  assignments: z.array(assignmentSchema),
  count: z.number(),
});
const nurtureOutput = z.object({ queued: z.number() });

export const leadGenerationModule: WorkflowModule<ShowcaseModuleDeps> = {
  meta: {
    id: WORKFLOW_ID,
    title: 'Lead Generation',
    description:
      'Score inbound leads, branch hot vs cold, and assign hot leads to reps after review. Shows conditional branching plus an egress-allowlisted, rate-limited CRM write.',
    sampleInput: {
      leads: [
        {
          name: 'Dana Ito',
          title: 'VP Engineering',
          company: 'Acme',
          companySize: 400,
        },
        {
          name: 'Lee Poe',
          title: 'Engineer',
          company: 'Globex',
          companySize: 40,
        },
      ],
    },
  },
  register({ createWorkflow, createStep, audit, deps }) {
    const { crm } = deps;

    // The gated write. egress declares the one host it may reach; rateLimit caps
    // it at 5/min (only actual executions consume budget). Binding-gated: no CRM
    // binding => log the assignment envelope and report 'simulated'.
    const crmAssign = createConnector<
      { assignments: Array<{ name: string; company: string }> },
      { assigned: number; outcome: 'assigned' | 'simulated' }
    >({
      id: CRM_ASSIGN_CONNECTOR,
      description: 'Assigns hot leads to sales reps via the CRM',
      inputSchema: z.object({ assignments: z.array(assignmentSchema) }),
      outputSchema: z.object({
        assigned: z.number(),
        outcome: z.enum(['assigned', 'simulated']),
      }),
      permissions: {
        sideEffect: 'write',
        requiresApproval: true,
        egress: [CRM_HOST],
        rateLimit: '5/min',
      },
      // tenantIsolation: scope-less calls deny instead of collapsing to
      // unsegmented keys — mandatory on a multi-tenant host (see gtm-outbound).
      policies: {
        audit,
        networkEgress: { allowedDomains: [CRM_HOST] },
        rateLimitStore: deps.rateLimit,
        evaluators: [tenantIsolation()],
      },
      execute: async ({ assignments }) => {
        if (!crm) {
          console.log(
            JSON.stringify({
              type: 'crm-assign-preview',
              count: assignments.length,
            }),
          );
          return { assigned: 0, outcome: 'simulated' as const };
        }
        const response = await crm.fetch(crm.endpoint, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            ...(crm.token ? { authorization: `Bearer ${crm.token}` } : {}),
          },
          body: JSON.stringify({ assignments }),
        });
        if (!response.ok) {
          throw new Error(
            `CRM assign failed: ${response.status} ${await response.text()}`,
          );
        }
        return { assigned: assignments.length, outcome: 'assigned' as const };
      },
    });

    const scoreLeads = createStep({
      id: 'scoreLeads',
      inputSchema: z.object({ leads: z.array(leadSchema) }),
      outputSchema: scoredOutput,
      execute: async ({ inputData }) => {
        const hot = inputData.leads.filter(isHot);
        const cold = inputData.leads.filter((lead) => !isHot(lead));
        return { hot, cold };
      },
    });

    // Branch targets. Each receives scoreLeads' output; the fan-in is keyed by
    // whichever branch(es) matched (both run when there are hot AND cold leads).
    const fastTrack = createStep({
      id: 'fastTrack',
      inputSchema: scoredOutput,
      outputSchema: fastTrackOutput,
      execute: async ({ inputData }) => ({
        assignments: inputData.hot.map((lead) => ({
          name: lead.name,
          company: lead.company,
        })),
        count: inputData.hot.length,
      }),
    });
    const nurture = createStep({
      id: 'nurture',
      inputSchema: scoredOutput,
      outputSchema: nurtureOutput,
      execute: async ({ inputData }) => ({ queued: inputData.cold.length }),
    });

    // Gate AFTER the branch fan-in; inputData is keyed by the matched branch
    // ids (both optional). Carries the hot assignments through to the write.
    const reviewHotLeads = createStep({
      id: 'reviewHotLeads',
      inputSchema: z.object({
        fastTrack: fastTrackOutput.optional(),
        nurture: nurtureOutput.optional(),
      }),
      outputSchema: z.object({
        approved: z.boolean(),
        assignments: z.array(assignmentSchema),
        decidedBy: z.string().optional(),
      }),
      suspendSchema: z.object({
        reason: z.string(),
        connectors: z.array(z.string()),
        hotCount: z.number(),
      }),
      resumeSchema: showcaseResumeSchema,
      execute: async ({ inputData, resumeData, suspend }) => {
        const assignments = inputData.fastTrack?.assignments ?? [];
        // No hot leads (empty or all-cold batch) => nothing to assign: skip the
        // gate AND the connector. Opening an approval for zero leads, or firing
        // an empty CRM write that burns rate-limit budget, would both be wrong.
        if (assignments.length === 0) {
          return { approved: false, assignments };
        }
        if (!resumeData) {
          return suspend({
            reason: 'human review required before assigning hot leads',
            connectors: [CRM_ASSIGN_CONNECTOR],
            hotCount: assignments.length,
          });
        }
        return {
          approved: resumeData.approved,
          assignments,
          decidedBy: resumeData.decidedBy,
        };
      },
    });

    const assignLeads = createStep({
      id: 'assignLeads',
      inputSchema: z.object({
        approved: z.boolean(),
        assignments: z.array(assignmentSchema),
        decidedBy: z.string().optional(),
      }),
      outputSchema: z.object({
        assigned: z.number(),
        outcome: z.enum(['assigned', 'simulated', 'declined']),
      }),
      execute: async ({ inputData, requestContext }) => {
        if (!inputData.approved) {
          return { assigned: 0, outcome: 'declined' as const };
        }
        return callConnector<
          { assignments: Array<{ name: string; company: string }> },
          { assigned: number; outcome: 'assigned' | 'simulated' }
        >(crmAssign, { assignments: inputData.assignments }, requestContext);
      },
    });

    createWorkflow({
      id: WORKFLOW_ID,
      inputSchema: z.object({ leads: z.array(leadSchema) }),
      outputSchema: z.object({
        assigned: z.number(),
        outcome: z.enum(['assigned', 'simulated', 'declined']),
      }),
    })
      .then(scoreLeads)
      .branch([
        [async ({ inputData }) => inputData.hot.length > 0, fastTrack],
        [async ({ inputData }) => inputData.cold.length > 0, nurture],
      ])
      .then(reviewHotLeads)
      .then(assignLeads)
      .commit();
  },
};
