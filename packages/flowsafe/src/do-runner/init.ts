// init()-based import-swap, the pattern @mastra/inngest and @mastra/temporal
// ship: init(...) returns backend-bound createWorkflow/createStep imported in
// place of the @mastra/core/workflows versions, leaving workflow definition
// code unchanged. Like @mastra/temporal, createStep passes through unmodified;
// createWorkflow registers the workflow on the runner runtime.
//
// init takes the Workers env (conventional `DB` D1 binding) or an explicit
// { storage } (tests, other adapters) — rather than raw env only — because
// binding names are deploy-specific and Workers env objects only exist inside
// handlers/DO constructors, never at module scope.

import type { MastraCompositeStore } from '@mastra/core/storage';
import type { AnyWorkflow } from '@mastra/core/workflows';
import {
  createWorkflow as coreCreateWorkflow,
  createStep,
} from '@mastra/core/workflows';

import type { D1DatabaseBinding } from './cf-types.js';
import { createD1Storage } from './d1-storage.js';
import type { ResumeLedger } from './resume-ledger.js';
import type { RequestContextProvider } from './runtime.js';
import { RunnerRuntime } from './runtime.js';

/** Workers env shape init() understands directly. */
export interface DORunnerEnv {
  DB: D1DatabaseBinding;
}

/** Explicit storage takes precedence over a DB binding when both are present. */
export type InitSource = { storage: MastraCompositeStore } | DORunnerEnv;

export interface InitOptions {
  /** Storage instance id when init builds the D1 store. Default: 'flowsafe'. */
  id?: string;
  /** Table name prefix when init builds the D1 store. */
  tablePrefix?: string;
  /**
   * Server-side requestContext source consulted on every start/resume — the
   * grant-minting seam. See RequestContextProvider in runtime.ts.
   */
  requestContextForRun?: RequestContextProvider;
  /**
   * Explicit resume ledger (see RunnerRuntimeOptions.resumeLedger). When
   * omitted, the runtime defaults to in-memory and the DO shell adopts a
   * ctx.storage-backed ledger.
   */
  resumeLedger?: ResumeLedger;
}

export interface InitResult {
  createWorkflow: typeof coreCreateWorkflow;
  createStep: typeof createStep;
  runtime: RunnerRuntime;
}

export function init(
  source: InitSource,
  options: InitOptions = {},
): InitResult {
  let storage: MastraCompositeStore;
  if ('storage' in source) {
    if (options.id !== undefined || options.tablePrefix !== undefined) {
      // Silently ignoring these would mask a misconfiguration: they only
      // apply when init builds the D1 store itself.
      throw new Error(
        "init: 'id'/'tablePrefix' apply only when init builds the D1 store from env.DB — configure the passed storage instance directly",
      );
    }
    storage = source.storage;
  } else {
    storage = createD1Storage({
      binding: source.DB,
      id: options.id,
      tablePrefix: options.tablePrefix,
    });
  }
  const runtime = new RunnerRuntime({
    storage,
    requestContextForRun: options.requestContextForRun,
    resumeLedger: options.resumeLedger,
  });

  // Cast preserves core's generic call-site inference (6 type params); the
  // wrapper body is inference-erased but only registers and returns.
  const boundCreateWorkflow = ((
    params: Parameters<typeof coreCreateWorkflow>[0],
  ) => {
    const workflow = coreCreateWorkflow(params);
    runtime.register(workflow as AnyWorkflow);
    return workflow;
  }) as typeof coreCreateWorkflow;

  return { createWorkflow: boundCreateWorkflow, createStep, runtime };
}
