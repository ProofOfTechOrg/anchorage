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
} from '../src/artifacts/index.js';
import { init, type RunnerRuntime } from '../src/do-runner/index.js';
import { assertWorkflowsRegistered } from '../src/host-kit/index.js';
import type { WorkflowModule } from '../src/host-kit/module.js';
import { accessRequestModule } from './workflows/access-request.js';
import { contentPipelineModule } from './workflows/content-pipeline.js';
import { gtmOutboundModule } from './workflows/gtm-outbound.js';
import { leadGenerationModule } from './workflows/lead-generation.js';
import { productLaunchModule } from './workflows/product-launch.js';
import type { ShowcaseDeps, ShowcaseModuleDeps } from './workflows/shared.js';

export type {
  EmailServiceBinding,
  ShowcaseDeps,
  ShowcaseModuleDeps,
} from './workflows/shared.js';

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
