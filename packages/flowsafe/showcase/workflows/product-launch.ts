// Module 4/5 — Product Launch: validate (with a dry-run pre-flight) → approve →
// deploy → confirm → promote. TWO approval gates, each minting a fresh grant
// leg-scoped to its own suspension (multi-checkpoint). Capabilities shown:
// multi-gate re-suspension through the host bridge, a destructive-class deploy
// connector, per-call idempotency (distinct keys for deploy vs promote), a
// binding-gated egress webhook, and a dry-run pre-flight (no side effect, no
// grant, no idempotency).

import {
  createConnector,
  DRY_RUN_CONTEXT_KEY,
  IDEMPOTENCY_KEY_CONTEXT_KEY,
} from '@proofoftech/breakwater';
import { z } from 'zod';

import type { WorkflowModule } from '../../src/host-kit/index.js';
import {
  callConnector,
  type ShowcaseModuleDeps,
  showcaseResumeSchema,
} from './shared.js';

export const DEPLOY_CONNECTOR = 'release-deploy';
/** The one host the deploy connector is allowed to reach (egress allowlist). */
const DEPLOY_HOST = 'deploy.example.com';
const WORKFLOW_ID = 'product-launch';

interface DeployInput {
  productName: string;
  version: string;
  phase: 'deploy' | 'promote';
}
interface DeployResult {
  ok: boolean;
  url: string;
  outcome: 'deployed' | 'simulated' | 'preview';
}

export const productLaunchModule: WorkflowModule<ShowcaseModuleDeps> = {
  meta: {
    id: WORKFLOW_ID,
    title: 'Product Launch',
    description:
      'Validate readiness with a dry-run pre-flight, deploy after approval, then promote to GA after a second confirmation gate. Shows multi-checkpoint approval, a destructive idempotent deploy, and dry-run.',
    sampleInput: { productName: 'anchorage', version: '1.0.0' },
  },
  register({ createWorkflow, createStep, audit, deps }) {
    const { deploy } = deps;

    // Destructive + idempotent + dry-run-capable. Two real invocations (deploy,
    // promote) use distinct idempotency keys so both execute; the pre-flight
    // uses dry-run, which returns before both the grant gate and the store.
    const releaseDeploy = createConnector<DeployInput, DeployResult>({
      id: DEPLOY_CONNECTOR,
      description:
        'Deploys or promotes a product release via the deploy webhook',
      inputSchema: z.object({
        productName: z.string(),
        version: z.string(),
        phase: z.enum(['deploy', 'promote']),
      }),
      outputSchema: z.object({
        ok: z.boolean(),
        url: z.string(),
        outcome: z.enum(['deployed', 'simulated', 'preview']),
      }),
      permissions: {
        sideEffect: 'destructive',
        requiresApproval: true,
        egress: [DEPLOY_HOST],
        idempotencyKey: true,
        dryRun: true,
      },
      policies: {
        audit,
        networkEgress: { allowedDomains: [DEPLOY_HOST] },
        idempotencyStore: deps.idempotency,
      },
      dryRunExecute: async ({ productName }) => ({
        ok: true,
        url: `https://${productName}.example.app`,
        outcome: 'preview' as const,
      }),
      execute: async ({ productName, version, phase }) => {
        if (!deploy) {
          console.log(
            JSON.stringify({
              type: 'deploy-preview',
              productName,
              version,
              phase,
            }),
          );
          return {
            ok: true,
            url: `https://${productName}.example.app`,
            outcome: 'simulated' as const,
          };
        }
        const response = await deploy.fetch(deploy.endpoint, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            ...(deploy.token
              ? { authorization: `Bearer ${deploy.token}` }
              : {}),
          },
          body: JSON.stringify({ productName, version, phase }),
        });
        if (!response.ok) {
          throw new Error(
            `deploy failed: ${response.status} ${await response.text()}`,
          );
        }
        return {
          ok: true,
          url: `https://${productName}.example.app`,
          outcome: 'deployed' as const,
        };
      },
    });

    const validateReadiness = createStep({
      id: 'validateReadiness',
      inputSchema: z.object({ productName: z.string(), version: z.string() }),
      outputSchema: z.object({
        productName: z.string(),
        version: z.string(),
        passed: z.boolean(),
        preflight: z.string(),
      }),
      execute: async ({ inputData, requestContext }) => {
        // A real readiness check: a name and a semver-ish version.
        const passed =
          inputData.productName.length > 0 &&
          /^\d+\.\d+/.test(inputData.version);
        // Dry-run pre-flight: simulate the deploy (no side effect, no grant, no
        // idempotency reservation) to confirm the connector accepts the release.
        const preflight = await callConnector<DeployInput, DeployResult>(
          releaseDeploy,
          {
            productName: inputData.productName,
            version: inputData.version,
            phase: 'deploy',
          },
          requestContext,
          { [DRY_RUN_CONTEXT_KEY]: true },
        );
        return {
          productName: inputData.productName,
          version: inputData.version,
          passed,
          preflight: preflight.outcome,
        };
      },
    });

    // Gate 1.
    const approveLaunch = createStep({
      id: 'approveLaunch',
      inputSchema: z.object({
        productName: z.string(),
        version: z.string(),
        passed: z.boolean(),
        preflight: z.string(),
      }),
      outputSchema: z.object({
        productName: z.string(),
        version: z.string(),
        approved: z.boolean(),
        decidedBy: z.string().optional(),
      }),
      suspendSchema: z.object({
        reason: z.string(),
        connectors: z.array(z.string()),
        passed: z.boolean(),
      }),
      resumeSchema: showcaseResumeSchema,
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          return suspend({
            reason: 'approval required before deploying the launch',
            connectors: [DEPLOY_CONNECTOR],
            passed: inputData.passed,
          });
        }
        return {
          productName: inputData.productName,
          version: inputData.version,
          approved: resumeData.approved,
          decidedBy: resumeData.decidedBy,
        };
      },
    });

    const executeLaunch = createStep({
      id: 'executeLaunch',
      inputSchema: z.object({
        productName: z.string(),
        version: z.string(),
        approved: z.boolean(),
        decidedBy: z.string().optional(),
      }),
      outputSchema: z.object({
        productName: z.string(),
        version: z.string(),
        deployed: z.boolean(),
        url: z.string(),
      }),
      execute: async ({ inputData, requestContext, runId }) => {
        if (!inputData.approved) {
          return {
            productName: inputData.productName,
            version: inputData.version,
            deployed: false,
            url: '',
          };
        }
        // Idempotency key includes runId so a same-run retry dedupes while two
        // independently-approved launches of the same product+version don't
        // collide (each is a distinct authorized deploy).
        const result = await callConnector<DeployInput, DeployResult>(
          releaseDeploy,
          {
            productName: inputData.productName,
            version: inputData.version,
            phase: 'deploy',
          },
          requestContext,
          {
            [IDEMPOTENCY_KEY_CONTEXT_KEY]: `${runId}:${inputData.productName}:${inputData.version}:deploy`,
          },
        );
        return {
          productName: inputData.productName,
          version: inputData.version,
          deployed: result.ok,
          url: result.url,
        };
      },
    });

    // Gate 2 — proves multi-checkpoint re-suspension: the run suspends AGAIN,
    // the host bridge re-queues a fresh approval, and its grant is minted
    // leg-scoped to THIS gate (not gate 1's).
    const confirmRollout = createStep({
      id: 'confirmRollout',
      inputSchema: z.object({
        productName: z.string(),
        version: z.string(),
        deployed: z.boolean(),
        url: z.string(),
      }),
      outputSchema: z.object({
        productName: z.string(),
        version: z.string(),
        url: z.string(),
        confirmed: z.boolean(),
        decidedBy: z.string().optional(),
      }),
      suspendSchema: z.object({
        reason: z.string(),
        connectors: z.array(z.string()),
        url: z.string(),
      }),
      resumeSchema: showcaseResumeSchema,
      execute: async ({ inputData, resumeData, suspend }) => {
        // The deploy gate was rejected: executeLaunch returned deployed:false
        // with no side effect, so there is nothing to promote. Skip this gate
        // AND its approval — suspending here would let the host bridge queue a
        // second approval whose approval would promote a run whose deploy never
        // happened. Decline straight through: completeLaunch returns
        // outcome:'declined' and never promotes. (Guarding on `deployed` rather
        // than the gate-1 decision also fail-safes any future path where a
        // deploy reports failure without throwing.)
        if (!inputData.deployed) {
          return {
            productName: inputData.productName,
            version: inputData.version,
            url: inputData.url,
            confirmed: false,
          };
        }
        if (!resumeData) {
          return suspend({
            reason: 'confirm rollout before promoting to GA',
            connectors: [DEPLOY_CONNECTOR],
            url: inputData.url,
          });
        }
        return {
          productName: inputData.productName,
          version: inputData.version,
          url: inputData.url,
          confirmed: resumeData.approved,
          decidedBy: resumeData.decidedBy,
        };
      },
    });

    const completeLaunch = createStep({
      id: 'completeLaunch',
      inputSchema: z.object({
        productName: z.string(),
        version: z.string(),
        url: z.string(),
        confirmed: z.boolean(),
        decidedBy: z.string().optional(),
      }),
      outputSchema: z.object({
        healthy: z.boolean(),
        url: z.string(),
        outcome: z.enum(['deployed', 'simulated', 'preview', 'declined']),
      }),
      execute: async ({ inputData, requestContext, runId }) => {
        if (!inputData.confirmed) {
          return {
            healthy: false,
            url: inputData.url,
            outcome: 'declined' as const,
          };
        }
        const result = await callConnector<DeployInput, DeployResult>(
          releaseDeploy,
          {
            productName: inputData.productName,
            version: inputData.version,
            phase: 'promote',
          },
          requestContext,
          {
            [IDEMPOTENCY_KEY_CONTEXT_KEY]: `${runId}:${inputData.productName}:${inputData.version}:promote`,
          },
        );
        return { healthy: result.ok, url: result.url, outcome: result.outcome };
      },
    });

    createWorkflow({
      id: WORKFLOW_ID,
      inputSchema: z.object({ productName: z.string(), version: z.string() }),
      outputSchema: z.object({
        healthy: z.boolean(),
        url: z.string(),
        outcome: z.enum(['deployed', 'simulated', 'preview', 'declined']),
      }),
    })
      .then(validateReadiness)
      .then(approveLaunch)
      .then(executeLaunch)
      .then(confirmRollout)
      .then(completeLaunch)
      .commit();
  },
};
