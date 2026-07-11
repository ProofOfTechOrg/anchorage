// Structural subsets of the Cloudflare Workers runtime types the DO runner
// forwards (D1Database) or reads from (DurableObjectState).
// @cloudflare/workers-types is a devDependency only — its types are ambient,
// never runtime code — so importing it from an exported signature here would
// leak into the published dist/**/*.d.ts and force every consumer to install
// it too (or silently degrade flagship types like D1Database to `any` under
// skipLibCheck, the operative mode). Same posture as SnapshotDatabase in
// d1-storage.ts, IdempotencyDatabase in breakwater, and ApprovalDatabase in
// approval-api: each interface below covers only the members do-runner
// actually reads or forwards, held structurally so tests can back them with
// plain objects and Workers pass the real bindings straight through.

import type { D1Database, DurableObjectState } from '@cloudflare/workers-types';

import type { ResumeLedgerStorage } from './resume-ledger.js';

/**
 * Structural subset of D1Database — the binding init() and createD1Storage
 * forward, opaque, into @mastra/cloudflare-d1's D1Store (see the cast at
 * that boundary in d1-storage.ts). do-runner itself never calls a method on
 * it — D1Store does, internally — so `prepare` is kept only as the one
 * identifying member: it rejects a value that plainly isn't D1-shaped (a
 * string, a KV namespace, ...) at the type level without over-specifying a
 * surface nothing here consumes.
 */
export interface D1DatabaseBinding {
  prepare(query: string): unknown;
}

/**
 * Structural subset of DurableObjectState — the shape DurableObjectRunner
 * reads from. Only `id.name` (tenant/run identity recovery off the DO's own
 * idFromName address, INV-1) and `storage` (the ctx.storage-backed resume
 * ledger — DurableStorageResumeLedger in resume-ledger.ts) are ever touched.
 */
export interface DurableObjectRunnerState {
  readonly id: {
    readonly name?: string;
  };
  readonly storage: ResumeLedgerStorage;
}

// Compile-time proof that the real Cloudflare types satisfy the structural
// subsets above, so a host passes env.DB / ctx straight through with no
// adapter. Type-only (erased at build; neither this import nor the
// non-exported aliases below reach the emitted .d.ts, so consumers pull no
// workers-types dependency) — the same technique artifacts/index.ts uses to
// pin R2Bucket against ArtifactBucket.
type AssertTrue<T extends true> = T;
type _D1DatabaseSatisfiesBinding = AssertTrue<
  D1Database extends D1DatabaseBinding ? true : false
>;
type _DurableObjectStateSatisfiesRunnerState = AssertTrue<
  DurableObjectState extends DurableObjectRunnerState ? true : false
>;
