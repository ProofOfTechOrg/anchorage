// Module 1/5 — GTM Outbound: serial pipeline, one approval gate, a binding-gated
// Cloudflare Email Service send. Ported from the original gtm-app pipeline.ts,
// restructured as a WorkflowModule so buildShowcaseRuntime registers it beside
// the other four. Capabilities shown: write-approval grant (the send's fail-
// closed spine), binding-gated real side effect, connector audit.

import { createConnector, tenantIsolation } from '@proofoftech/breakwater';
import { z } from 'zod';

import type { WorkflowModule } from '../../src/host-kit/module.js';
import {
  callConnector,
  type ShowcaseModuleDeps,
  showcaseResumeSchema,
} from './shared.js';

/** The connector id the send step demands a grant for; the gate mints it. */
export const OUTREACH_CONNECTOR = 'outreach-email';
const WORKFLOW_ID = 'gtm-outbound';

/** One personalized outreach email; a run approves and sends a batch of these. */
export interface OutreachDraft {
  to: string;
  subject: string;
  body: string;
}

const outreachDraftSchema = z.object({
  to: z.string(),
  subject: z.string(),
  body: z.string(),
});

// 'sent' = live delivery, 'simulated' = no Email binding (envelope logged),
// 'declined' = reviewer rejected (connector never invoked). `delivered` counts
// live deliveries only.
const CONNECTOR_OUTCOMES = ['sent', 'simulated'] as const;
const STEP_OUTCOMES = ['sent', 'simulated', 'declined'] as const;
type ConnectorOutcome = (typeof CONNECTOR_OUTCOMES)[number];

interface SendResult {
  outcome: ConnectorOutcome;
  delivered: number;
}

export const gtmOutboundModule: WorkflowModule<ShowcaseModuleDeps> = {
  meta: {
    id: WORKFLOW_ID,
    title: 'GTM Outbound',
    description:
      'Research target accounts, draft personalized outreach, and send the batch after human approval. One approval gate; the send is a binding-gated Cloudflare Email Service call (simulated offline).',
    sampleInput: { industry: 'fintech', targetCount: 50 },
  },
  register({ createWorkflow, createStep, audit, deps }) {
    const { fromAddress, fromName, email } = deps;

    // The one true side effect. requiresApproval => the flowsafe grant is the
    // only token that admits execute; reaching execute at all proves the grant
    // was minted. Binding-gated: no Email Service binding => render + log the
    // envelope and report 'simulated', still running the real execute so the
    // grant gate is exercised (a forged resume fails closed).
    const outreachEmail = createConnector<
      { drafts: OutreachDraft[] },
      SendResult
    >({
      id: OUTREACH_CONNECTOR,
      description:
        'Sends the approved outreach batch via Cloudflare Email Service',
      inputSchema: z.object({ drafts: z.array(outreachDraftSchema) }),
      outputSchema: z.object({
        outcome: z.enum(CONNECTOR_OUTCOMES),
        delivered: z.number(),
      }),
      permissions: { sideEffect: 'write', requiresApproval: true },
      // tenantIsolation: the showcase is multi-tenant, so a connector call
      // whose requestContext somehow lacks the runtime-minted isolation
      // scope must DENY rather than fall back to unsegmented single-tenant
      // idempotency/rate-limit keys (CONNECTORS.md makes the evaluator
      // mandatory for multi-tenant hosts). Every showcase connector
      // registers it.
      policies: { audit, evaluators: [tenantIsolation()] },
      execute: async ({ drafts }) => {
        if (!email) {
          for (const draft of drafts) {
            console.log(
              JSON.stringify({
                type: 'outreach-preview',
                to: draft.to,
                from: fromAddress,
                subject: draft.subject,
              }),
            );
          }
          return { outcome: 'simulated' as const, delivered: 0 };
        }
        // Live send. Partial-batch honesty: on a mid-batch throw report how
        // many already went out so an operator sees 2/5 delivered, not a bare
        // failure, and a retry knows where it stopped.
        let delivered = 0;
        for (const draft of drafts) {
          try {
            await email.send({
              to: draft.to,
              from: { email: fromAddress, name: fromName },
              subject: draft.subject,
              html: `<p>${draft.body}</p>`,
              text: draft.body,
            });
          } catch (cause) {
            throw new Error(
              `outreach send failed after ${delivered}/${drafts.length} delivered (to ${draft.to}): ${cause instanceof Error ? cause.message : String(cause)}`,
            );
          }
          delivered += 1;
          console.log(
            JSON.stringify({
              type: 'outreach-sent',
              to: draft.to,
              from: fromAddress,
              subject: draft.subject,
            }),
          );
        }
        return { outcome: 'sent' as const, delivered };
      },
    });

    const researchAccounts = createStep({
      id: 'researchAccounts',
      inputSchema: z.object({
        industry: z.string(),
        targetCount: z.number().default(50),
      }),
      outputSchema: z.object({ accounts: z.array(z.string()) }),
      execute: async ({ inputData }) => {
        console.log(
          JSON.stringify({
            type: 'research',
            industry: inputData.industry,
            targetCount: inputData.targetCount,
          }),
        );
        return { accounts: ['Acme Corp', 'Globex Inc'] };
      },
    });

    const enrichContacts = createStep({
      id: 'enrichContacts',
      inputSchema: z.object({ accounts: z.array(z.string()) }),
      outputSchema: z.object({ contacts: z.array(z.string()) }),
      execute: async ({ inputData }) => ({
        contacts: inputData.accounts.map(
          (account) =>
            `head-of-growth@${account.toLowerCase().replace(/\s+/g, '')}.com`,
        ),
      }),
    });

    const generateOutreach = createStep({
      id: 'generateOutreach',
      inputSchema: z.object({ contacts: z.array(z.string()) }),
      outputSchema: z.object({ drafts: z.array(outreachDraftSchema) }),
      execute: async ({ inputData }) => ({
        drafts: inputData.contacts.map((contact) => ({
          to: contact,
          subject: 'Quick question about your growth stack',
          body: `Hi ${contact}, noticed your team is scaling — worth a chat?`,
        })),
      }),
    });

    // The real suspend gate. It suspends with `connectors: [OUTREACH_CONNECTOR]`;
    // the host bridge copies that into the queued approval, so a decision mints
    // exactly this grant. resumeSchema matches approval-api's defaultResumeData.
    const reviewAndApprove = createStep({
      id: 'reviewAndApprove',
      inputSchema: z.object({ drafts: z.array(outreachDraftSchema) }),
      outputSchema: z.object({
        drafts: z.array(outreachDraftSchema),
        approved: z.boolean(),
        decidedBy: z.string().optional(),
      }),
      suspendSchema: z.object({
        reason: z.string(),
        connectors: z.array(z.string()),
        draftCount: z.number(),
      }),
      resumeSchema: showcaseResumeSchema,
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          return suspend({
            reason: 'human approval required before outreach send',
            connectors: [OUTREACH_CONNECTOR],
            draftCount: inputData.drafts.length,
          });
        }
        return {
          drafts: inputData.drafts,
          approved: resumeData.approved,
          decidedBy: resumeData.decidedBy,
        };
      },
    });

    // The gated write. Forward the runtime-supplied requestContext into the
    // connector; never set the grant key by hand. A forged resume reaches here
    // with no grant and the wrapper throws (fail closed).
    const sendOutreach = createStep({
      id: 'sendOutreach',
      inputSchema: z.object({
        drafts: z.array(outreachDraftSchema),
        approved: z.boolean(),
        decidedBy: z.string().optional(),
      }),
      outputSchema: z.object({
        outcome: z.enum(STEP_OUTCOMES),
        delivered: z.number(),
      }),
      execute: async ({ inputData, requestContext }) => {
        if (!inputData.approved) {
          return { outcome: 'declined' as const, delivered: 0 };
        }
        return callConnector<{ drafts: OutreachDraft[] }, SendResult>(
          outreachEmail,
          { drafts: inputData.drafts },
          requestContext,
        );
      },
    });

    createWorkflow({
      id: WORKFLOW_ID,
      inputSchema: z.object({
        industry: z.string(),
        targetCount: z.number().default(50),
      }),
      outputSchema: z.object({
        outcome: z.enum(STEP_OUTCOMES),
        delivered: z.number(),
      }),
    })
      .then(researchAccounts)
      .then(enrichContacts)
      .then(generateOutreach)
      .then(reviewAndApprove)
      .then(sendOutreach)
      .commit();
  },
};
