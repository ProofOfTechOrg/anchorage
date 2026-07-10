// buildShowcaseRuntime — the one place all five workflow modules are registered
// onto a single RunnerRuntime whose requestContext is minted from APPROVED
// approval records. One init(), one Mastra instance, one DO class, one D1.
// register-before-first-run is satisfied because this builder registers every
// module synchronously before returning the runtime.

import {
  AuditLogger,
  InMemoryIdempotencyStore,
  InMemoryRateLimitStore,
} from '@proofoftech/breakwater';

import {
  InMemoryArtifactBucket,
  R2ArtifactStore,
} from '@proofoftech/flowsafe/artifacts';
import { init, type RunnerRuntime } from '@proofoftech/flowsafe/do-runner';
import { assertWorkflowsRegistered } from '@proofoftech/flowsafe/host-kit';
import type { WorkflowModule } from '@proofoftech/flowsafe/host-kit/module';
import { accessRequestModule } from '#worker/workflows/access-request';
import { contentPipelineModule } from '#worker/workflows/content-pipeline';
import { gtmOutboundModule } from '#worker/workflows/gtm-outbound';
import { leadGenerationModule } from '#worker/workflows/lead-generation';
import { productLaunchModule } from '#worker/workflows/product-launch';
import type {
  ShowcaseDeps,
  ShowcaseModuleDeps,
} from '#worker/workflows/shared';

export type {
  EmailServiceBinding,
  ShowcaseDeps,
  ShowcaseModuleDeps,
} from '#worker/workflows/shared';

/** Every showcase workflow module, in launcher/registration order. */
export const SHOWCASE_MODULES: ReadonlyArray<
  WorkflowModule<ShowcaseModuleDeps>
> = [
  gtmOutboundModule,
  contentPipelineModule,
  leadGenerationModule,
  productLaunchModule,
  accessRequestModule,
];

const DEFAULT_FROM_ADDRESS = 'gtm@example.com';
const DEFAULT_FROM_NAME = 'Anchorage Showcase';

/**
 * Build the showcase runtime: resolve binding-gated infra to concrete stores
 * (offline defaults are real, not fakes — InMemory artifact bucket + idempotency
 * + rate-limit), wire the grant-minting seam, and register all five modules.
 */
export function buildShowcaseRuntime(deps: ShowcaseDeps): RunnerRuntime {
  const audit = deps.audit ?? new AuditLogger();

  const moduleDeps: ShowcaseModuleDeps = {
    email: deps.email,
    fromAddress: deps.fromAddress || DEFAULT_FROM_ADDRESS,
    fromName: deps.fromName || DEFAULT_FROM_NAME,
    artifactStore: new R2ArtifactStore(
      deps.artifactBucket ?? new InMemoryArtifactBucket(),
    ),
    // Shared across the modules: the connector SDK scopes idempotency + rate
    // budgets by connector id (and each module keys idempotency by runId too),
    // so sharing is safe as long as connector ids are globally unique — they are.
    idempotency: deps.idempotencyStore ?? new InMemoryIdempotencyStore(),
    rateLimit: deps.rateLimitStore ?? new InMemoryRateLimitStore(),
    crm: deps.crm,
    deploy: deps.deploy,
  };

  const { createWorkflow, createStep, runtime } = init(deps.initInput, {
    // The grant-minting seam: on every start/resume the runtime derives the
    // breakwater grant key from APPROVED records — decisions become
    // capabilities without any grant crossing a request body. The provider is
    // host-built (see ShowcaseDeps.grantProvider) so the store binding matches
    // the host's topology.
    requestContextForRun: deps.grantProvider,
  });

  for (const workflowModule of SHOWCASE_MODULES) {
    workflowModule.register({
      createWorkflow,
      createStep,
      audit,
      deps: moduleDeps,
    });
  }

  // Fail fast if a module committed its workflow under an id different from its
  // meta.id: the launcher + per-workflow RBAC look runs up by meta.id, but the
  // runtime routes start/resume by the committed createWorkflow id.
  assertWorkflowsRegistered(
    runtime,
    SHOWCASE_MODULES.map((entry) => entry.meta),
  );

  return runtime;
}
