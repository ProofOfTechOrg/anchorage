// Module 6/6 — Wire Transfer: the guardrails control room's server-backed
// scenario. An agent-prepared payment must clear a human approval gate before
// the payment connector may fire. Capabilities shown: the write-approval grant
// (requiresApproval — the resumed leg re-derives the grant from APPROVED
// records; a forged resume reaches the connector with no grant and is denied),
// tenant isolation on the connector's scope keys, and separation of duties at
// the queue (the requester cannot release their own wire). The connector is
// binding-gated like every showcase side effect: no payment rail is wired, so
// execute records the envelope and returns a simulated confirmation while the
// grant gate stays fully real.

import { createConnector, tenantIsolation } from '@proofoftech/breakwater';
import type { WorkflowModule } from '@proofoftech/flowsafe/host-kit/module';
import { z } from 'zod';
import {
  callConnector,
  type ShowcaseModuleDeps,
  showcaseResumeSchema,
} from '#worker/workflows/shared';

export const WIRE_CONNECTOR = 'release-wire';
const WORKFLOW_ID = 'wire-transfer';

/** Transfers at or above this amount get a review flag in the gate reason. */
const HIGH_VALUE_THRESHOLD = 10_000;

export const wireTransferModule: WorkflowModule<ShowcaseModuleDeps> = {
  meta: {
    id: WORKFLOW_ID,
    title: 'Wire Transfer',
    description:
      'An agent prepares a payment release that pauses at a human approval gate. The release connector runs only under a grant derived from the APPROVED record, so a forged resume is denied, and the requester cannot approve their own wire (SoD).',
    sampleInput: {
      amount: 25000,
      currency: 'USD',
      beneficiary: 'Northwind Metals Ltd',
      reference: 'INV-2311',
    },
  },
  register({ createWorkflow, createStep, audit }) {
    // The gated write. No payment binding exists, so execute is a simulation
    // by construction — but it still runs the real wrapper: the approval
    // grant, tenant isolation, and audit all fire exactly as they would with
    // a live rail behind it.
    const releaseWireConnector = createConnector<
      {
        amount: number;
        currency: string;
        beneficiary: string;
        reference: string;
      },
      { released: boolean; confirmation: string; reference: string }
    >({
      id: WIRE_CONNECTOR,
      description: 'Releases an approved wire transfer (simulated rail)',
      inputSchema: z.object({
        amount: z.number().positive(),
        currency: z.string(),
        beneficiary: z.string(),
        reference: z.string(),
      }),
      outputSchema: z.object({
        released: z.boolean(),
        confirmation: z.string(),
        reference: z.string(),
      }),
      permissions: { sideEffect: 'write', requiresApproval: true },
      policies: {
        audit,
        // Mandatory on this multi-tenant host: a scope-less call denies
        // instead of collapsing to unsegmented keys (see gtm-outbound).
        evaluators: [tenantIsolation()],
      },
      execute: async ({ amount, currency, beneficiary, reference }) => {
        console.log(
          JSON.stringify({
            type: 'wire-released',
            amount,
            currency,
            beneficiary,
            reference,
          }),
        );
        return {
          released: true,
          confirmation: `sim-${reference.toLowerCase()}`,
          reference,
        };
      },
    });

    const prepareTransfer = createStep({
      id: 'prepareTransfer',
      inputSchema: z.object({
        amount: z.number().positive(),
        currency: z.string(),
        beneficiary: z.string(),
        reference: z.string(),
      }),
      outputSchema: z.object({
        amount: z.number(),
        currency: z.string(),
        beneficiary: z.string(),
        reference: z.string(),
        highValue: z.boolean(),
      }),
      execute: async ({ inputData }) => ({
        amount: inputData.amount,
        currency: inputData.currency.trim().toUpperCase(),
        beneficiary: inputData.beneficiary.trim(),
        reference: inputData.reference.trim(),
        highValue: inputData.amount >= HIGH_VALUE_THRESHOLD,
      }),
    });

    const approveTransfer = createStep({
      id: 'approveTransfer',
      inputSchema: z.object({
        amount: z.number(),
        currency: z.string(),
        beneficiary: z.string(),
        reference: z.string(),
        highValue: z.boolean(),
      }),
      outputSchema: z.object({
        amount: z.number(),
        currency: z.string(),
        beneficiary: z.string(),
        reference: z.string(),
        approved: z.boolean(),
        decidedBy: z.string().optional(),
      }),
      suspendSchema: z.object({
        reason: z.string(),
        connectors: z.array(z.string()),
        amount: z.number(),
        currency: z.string(),
        beneficiary: z.string(),
      }),
      resumeSchema: showcaseResumeSchema,
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          const flag = inputData.highValue ? ' · high value' : '';
          return suspend({
            reason: `release ${inputData.currency} ${inputData.amount} to ${inputData.beneficiary} (${inputData.reference})${flag}`,
            connectors: [WIRE_CONNECTOR],
            amount: inputData.amount,
            currency: inputData.currency,
            beneficiary: inputData.beneficiary,
          });
        }
        return {
          amount: inputData.amount,
          currency: inputData.currency,
          beneficiary: inputData.beneficiary,
          reference: inputData.reference,
          approved: resumeData.approved,
          decidedBy: resumeData.decidedBy,
        };
      },
    });

    const releaseFunds = createStep({
      id: 'releaseFunds',
      inputSchema: z.object({
        amount: z.number(),
        currency: z.string(),
        beneficiary: z.string(),
        reference: z.string(),
        approved: z.boolean(),
        decidedBy: z.string().optional(),
      }),
      outputSchema: z.object({
        released: z.boolean(),
        confirmation: z.string().optional(),
        reference: z.string(),
      }),
      execute: async ({ inputData, requestContext }) => {
        if (!inputData.approved) {
          return { released: false, reference: inputData.reference };
        }
        return callConnector<
          {
            amount: number;
            currency: string;
            beneficiary: string;
            reference: string;
          },
          { released: boolean; confirmation: string; reference: string }
        >(
          releaseWireConnector,
          {
            amount: inputData.amount,
            currency: inputData.currency,
            beneficiary: inputData.beneficiary,
            reference: inputData.reference,
          },
          requestContext,
        );
      },
    });

    createWorkflow({
      id: WORKFLOW_ID,
      inputSchema: z.object({
        amount: z.number().positive(),
        currency: z.string(),
        beneficiary: z.string(),
        reference: z.string(),
      }),
      outputSchema: z.object({
        released: z.boolean(),
        confirmation: z.string().optional(),
        reference: z.string(),
      }),
    })
      .then(prepareTransfer)
      .then(approveTransfer)
      .then(releaseFunds)
      .commit();
  },
};
