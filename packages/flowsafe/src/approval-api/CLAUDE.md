# approval-api/

Phase 3 approval queue: CAS-backed store, role-authorized service, REST
router, and the grant-minting seam.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `README.md` | Invisible knowledge: the suspend→decide→grant loop, design decisions (derivation-not-transport, suspension scoping, contract mirror, CAS-only), and the invariants bridges/providers must uphold | Before changing grant semantics, store transitions, or the breakwater contract |
| `contract.ts` | Mirrored breakwater wire contract: re-exports the requestContext key literals from `do-runner/breakwater-keys.ts` (approved-connectors, actor, workflow-scope), plus `ApprovalRole`/`ApprovalActor`, `ApprovalAuditEvent`/`ApprovalAuditSink` | Changing the cross-package contract (must stay literal-equal to breakwater's constants — enforced by `end-to-end.test.ts`) |
| `types.ts` | `ApprovalRecord` (incl. the `suspendedAt`/`resumedAt` pair — the exact suspension a step-keyed approval binds to), statuses/priorities (`OPEN_STATUSES`, `APPROVAL_STATUSES`), `CreateApprovalInput`, `ApprovalListFilter`, `ApprovalMetrics`, decide wire results (`DecideResult`, `ResumeOutcome`) | Changing the record shape, status set, metrics fields, or decide result shape |
| `store.ts` | `ApprovalStore` contract (CAS `transition`, idempotent `create`, FIFO `list`) + `InMemoryApprovalStore`; `stepKeyOf` | Changing store semantics — both stores must keep passing the shared contract suite |
| `d1-store.ts` | `D1ApprovalStore`: schema DDL (partial unique open-step index, `suspended_at`/`resumed_at` columns each with a defensive per-column ALTER backfill for pre-existing DBs — spike-era and the suspended_at-only release), `UPDATE ... RETURNING` CAS, lazy memoized schema creation (`#ready`/`#createSchema`); `ApprovalDatabase` structural D1 subset | Changing the schema or SQL |
| `service.ts` | `ApprovalService`: role policy, self-approval gate, claim/decide/delegate/create/metrics/sweepSLA, audit emission, `resumeRun` callback, typed HTTP-mappable errors | Changing business rules, role policy, or SLA behavior |
| `router.ts` | `createApprovalRouter`: fetch routing for the REST endpoints, injected `authenticate`, error→status mapping (400/401/403/404/409) | Adding or changing endpoints or status codes |
| `grants.ts` | `approvedConnectorsForLeg`, `approvalGrantProvider` (the `requestContextForRun` implementation), `defaultResumeData`, `resumeViaRuntime`; suspension binding is exact-match-first on the `(suspendedAt, resumedAt)` pair (clock-free; `resumedAt` — undefined on a first suspension, defined on a re-suspension — is the categorical tie-breaker for same-ms `suspendedAt` collisions) with a legacy decidedAt-after fallback | Changing how decisions become grants or resumeData |
| `index.ts` | Subpackage barrel | Finding the public approval-api surface |
| `store.test.ts` | Shared store contract suite run against BOTH stores (D1 via a node:sqlite adapter), payload round-trips, SQL-injection inertness | Adding store tests |
| `service.test.ts` | Role matrix, CAS conflicts, self-approval, decide/resume outcomes, SLA boundary + sweep idempotency, metrics math, audit isolation | Adding service tests |
| `router.test.ts` | Endpoint routing + status-code mapping, auth, method/path edge cases | Adding router tests |
| `grants.test.ts` | Suspension-scoped derivation (shadowing, boundary, fail-closed), provider output, resume wiring | Adding grant tests |
| `end-to-end.test.ts` | Cross-package proof with breakwater from source: contract tripwires, forged-resume fail-closed, two-gate and re-suspension fixtures, approve/reject loops | Verifying the breakwater contract or the full loop |
