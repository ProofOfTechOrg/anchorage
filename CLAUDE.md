# CLAUDE.md

Instructions for Claude Code when working in this repository.

## Project Status

Roadmap Phases 1-4 implemented (see `mvp-roadmap.md`, gitignored/local-only).
Pinned to `@mastra/core` 1.50.0. CI (`.github/workflows/ci.yml`) runs the
verification gate + `spike:verify` on push/PR to `main`. Phases 1-3:

- breakwater: `PolicyEngine` + `RBACMiddleware` as real Mastra processors,
  `AuditLogger` shared sink (own module `src/audit/`, re-exported from
  `rbac` for compat) — tested (`pnpm -r test`).
- breakwater connector-sdk (Phase 2): `createConnector()` wraps Mastra
  `createTool()` with an enforced permission manifest — network-egress
  allowlist gate, write-approval gate (requestContext grant enforced on
  every path; Mastra-native `requireApproval` compiled as a per-call
  predicate for the agent pause UX — exempts dry-run requests, never a
  substitute for the grant), keyed idempotent replay (same-isolate twins
  join a placeholder registered before the store round-trip);
  tool-boundary evaluators in `policy-engine/tool-policy.ts`
  (`networkEgress`, `approvalRequired`) — tested. Enforcement contract in
  `docs/connector-interface.md`.
- flowsafe: DO runner (`init()` import-swap, `RunnerRuntime`,
  `DurableObjectRunner`) + D1 storage wrapper — tested, and spike-verified on
  workerd: suspend → dev-server restart → resume from the D1 snapshot. The
  proof is automated:
  `pnpm --filter @proofoftech/flowsafe spike:verify` (scoped kill protocol,
  restart persistence, fail-closed forged-resume probe, self-decision-denial
  probe; exits 0/1); `spike` stays the interactive server (demo in
  `packages/flowsafe/demo/`).
- flowsafe approval queue (Phase 3): `approval-api` — CAS-guarded store
  (D1 + in-memory), `ApprovalService` (role-checked claim/decide/delegate,
  SLA sweep → escalation, audit sink), fetch router for the REST surface,
  and the grant-minting seam: `approvalGrantProvider(tenantBoundStore)` wired
  into `RunnerRuntime.requestContextForRun` (hosts wire
  `approvalGrantProviderFromFactory(storeFactory)`, which recovers each leg's
  tenant from the runId prefix and binds the store to it) derives
  `breakwater.approvedConnectors` from APPROVED records on every
  start/resume, leg-scoped to the resumed step (grants never travel in HTTP
  bodies; forged resumes fail closed at the connector gate even for
  connectors approved at other gates of the run — proven in
  `approval-api/end-to-end.test.ts` and the workerd spike). Self-approval
  denied by default. `approval-ui` —
  React dashboard (queue/detail/metrics) + DOM-free `ApprovalApiClient`,
  subpath export only. **Styling-library agnostic**: views render through
  injected slot components (`ApprovalUIComponents` contract + `ApprovalUIProvider`
  in `components.tsx`) with a plain-HTML default; a headless
  `useApprovalDashboard` hook holds the logic. The library has no Astryx/CSS
  dependency and runs on React 18+. `status()` widened
  (result/error/suspendPayload/timestamps).
- flowsafe app (`packages/flowsafe/app/`): full Vite React app that consumes the
  library and injects an **Astryx adapter** (`app/src/astryx-components.tsx`) —
  the only place Astryx (Meta's design system) is used; it's a `devDependency`,
  so published consumers pull zero Astryx. `vite build` bundles the theme CSS,
  and `app:dev` mounts a live seeded approval-api in the dev server (real
  claim/decide/delegate). Run: `pnpm --filter @proofoftech/flowsafe app:dev`.
- Deferred backlog closed (2026-07-07, all 8 items): the policy engine gates
  output per channel — `answer`/`reasoning`/`object` (`OutputChannel`;
  policies declare `channels`, default answer-only; `denyPatterns` defaults
  to all three) — with per-stream state accumulation replacing the O(n²)
  rebuild, incremental `denyPatterns` scans, and opt-in zero-leak hold-back
  buffering (`PolicyEngineOptions.holdBack` + per-policy `holdBackChars`
  hints; held tails flush at `text-end`/`reasoning-end` with finish as the
  backstop, both riding the runner's reprocess convention — the zero-leak
  guarantee is per segment). Connector
  manifests enforce `dryRun` (per-call simulation via `DRY_RUN_CONTEXT_KEY`
  + `dryRunExecute`; unsupported manifests fail closed) and `rateLimit`
  ('<count>/<unit>' fixed windows against a `RateLimitStore`; only actual
  executions consume budget). Idempotency grew `AtomicIdempotencyStore`
  (reserve/release — the wrapper prefers it) and `D1IdempotencyStore`
  (INSERT-claim CAS + stale-pending TTL takeover). `crossWorkflowIsolation`
  ships as a tool policy reading `WORKFLOW_SCOPE_CONTEXT_KEY`
  ('breakwater.workflowScope'), which `RunnerRuntime` mints on every leg.
  flowsafe validates `workflowId` at `register()` (same path-safe pattern as
  runId), ships `purgeExpiredWorkflowRuns` (terminal-status-only TTL purge of
  `mastra_workflow_snapshot`; optional `artifactStore` pairs each purged run's
  R2 artifacts with its row — the row is the only record of their keys;
  missing table = zero rows; scheduling stays with the caller), and binds
  approvals to suspensions clock-free (`RunSummary.{suspendedAt,resumeCount}`
  maps → `ApprovalRecord.{suspendedAt,resumeCount}`, exact-match minting on the
  `(suspendedAt, resumeCount)` pair — `resumeCount` is the runtime-owned
  monotonic per-(run,step) resume ordinal, undefined on a first suspension and
  `1,2,…` on re-suspensions; the runtime increments it on every resume
  regardless of payload, so it stays present even on a no-payload re-suspension
  and never collides, keeping same-step suspensions distinct even when their
  `suspendedAt` stamps collide in one ms on the in-process path — with a legacy
  decidedAt-after fallback for records created without the capture. `resumedAt`
  is retained as informational audit metadata only (Mastra stamps it solely on
  a payload-bearing resume). This supersedes the earlier `(suspendedAt,
  resumedAt)` binding and closes its deep-3+-re-suspension residual (the counter
  strictly increments, so it never collides).

Phase 4 (Ecosystem, 2026-07-07):
- breakwater `src/agent-cli/`: Claude Code + Codex as approval-gated
  connectors built on `createConnector` (write-class, dry-run = command
  preview, injectable `AgentCliExec`, Node-only default runner via
  `getBuiltinModule`). `AgentCliDefinition.buildFlags` returns flags only;
  the wrapper appends `'--', input.prompt` and binds option values with
  `--flag=value` — argv flag-smuggling defense (a `-`-leading prompt/model
  can never be parsed as a CLI flag). Authoring guide: breakwater
  `CONNECTORS.md`.
- flowsafe `src/audit-export/`: Cloudflare Queues → SIEM export —
  `queueAuditSink` producer adapter + `createAuditQueueConsumer` (NDJSON
  batch POST, ack-on-2xx/retry-otherwise, transform seam). Structural
  Queue/MessageBatch subsets, no workers-types dep.
- flowsafe `src/artifacts/`: `R2ArtifactStore` over a structural
  `ArtifactBucket` seam (+ `InMemoryArtifactBucket`), keys
  `[prefix/]workflowId/runId/name` validated by the do-runner's exported
  `PATH_SAFE_ID_PATTERN` + segment-wise name checks; `deleteRun` pairs with
  retention purge.
- flowsafe `deploy/`: copy-ready production Worker (cron-owned SLA sweep + retention purge) —
  `scheduled()` runs `sweepSLA` + `purgeExpiredWorkflowRuns` on a cron
  trigger (isolated failures), bearer-token auth seam, start+resume approval
  bridges (multi-gate), optional Queues audit export. `deploy:cf`/`deploy:dev`.

Verification gate: `pnpm -r lint && pnpm -r typecheck && pnpm -r test && pnpm -r build` (698 tests).

Showcase (2026-07-09): all five `docs/examples/*` workflows made runnable behind
one React frontend and shipped as a single Cloudflare deploy —
`packages/flowsafe/showcase/` (`buildShowcaseRuntime` registers all 5 on one
Worker + DO + D1; `GET /workflows` + per-workflow `allowedRoles`; `assets` block
serves `app/dist` at `/` with the API on the same origin) + host-agnostic glue in
`src/host-kit/` (subpath export `./host-kit`): `WorkflowModule`/`WorkflowMeta`,
the approval bridge, the bearer auth seam (`parseActorTokens`,
`bearerActorAuthenticator`), and `createRunRouter({ resolve })` — the
`/workflows` + `/runs` surface with its 401 → INV-3 → coarse `RUN_START_ROLES`
→ per-workflow `allowedRoles` gate order; `resolve: TenantResolver`
(authenticate → validate → bind) replaces the bare `authenticate`, and
status/resume answer 404 for another tenant's run — no existence oracle. All four hosts (showcase, deploy template, `app:dev` plugin, demo
spike) consume it; each injects only its resume topology (DO stub vs in-process).
The `app/` frontend gains a launcher + run-status panel + actor switcher;
`run-api-dev-plugin.ts` runs the showcase host in-process for `app:dev`. The demo
bearer identities live once in `showcase/demo-actors.ts` (drift-tested against
`.dev.vars.example`); `showcase/wrangler.jsonc` bakes in NO credentials, so a
deploy 401s until `wrangler secret put APPROVAL_ACTOR_TOKENS`. Connectors stay
binding-gated (simulate offline; grant gate always exercised).

Multi-tenant platform (2026-07-09, `.notes/multi-tenant-and-demo-plan.md` implemented):
one tenant dimension, one chokepoint per resource class, fail-closed by construction.
Three invariants: **INV-1** — every runId is server-minted `${tenantId}_${uuid}` from the
AUTHENTICATED tenant (`createRunRouter` 400s client runIds; `RunnerRuntime.start` requires
one — no generation fallback; the DO asserts path ≡ `ctx.id.name`), making Mastra snapshot
rows, DO instances, `#runKey`, the grant-mint `runId` predicate, and R2 keys tenant-disjoint
with no schema or signature change. **INV-2** — approval stores are tenant-BOUND at
construction (`D1ApprovalStoreFactory`/`InMemoryApprovalStoreFactory.forTenant()`; the
`TENANT_BOUND` unique-symbol brand makes an unbound/system store a compile error in request
scope; `tenant_id` column + unconditional predicates; the open-step index was DROPped and
recreated tenant-first under a NEW name — name-keyed `IF NOT EXISTS` redefinition is a silent
no-op; a pre-tenant table REFUSES to serve). Requests flow through `TenantResolver`
(authenticate → INV-3-validate → bind) so no pre-auth store exists; `sweepSLA` left the
service for a cron-only function over `SystemApprovalStore` (the HTTP sweep route is gone).
**INV-3** — `tenantId` matches `^[a-z0-9]{3,32}$` (no chars in `[0x5F,0x60]`), making the
runId prefix ownership check and the `[tid_, tid\x60)` range purge EXACT
(character-exhaustive test pins it). Plus: durable `ctx.storage` resume ledger (eviction no
longer no-ops approved re-suspension resumes); `ISOLATION_SCOPE_CONTEXT_KEY` segments
breakwater idempotency/rate-limit keys per tenant (no flag — absent scope keeps single-tenant
keys; `tenantIsolation` evaluator denies scope-less calls incl. dry-run); `purgeTenant`
offboards all three stores (any-status snapshots + approvals + artifacts;
a missing snapshot table reads as empty so run-less tenants — expired demo
sandboxes — still offboard, and artifacts of retention-purged runs are covered
by that purge's own `artifactStore` pairing, not re-enumerable here); Mastra
six-table-inventory + `run_id` schema guards; the R2 no-workflow-level-listing pin; identity
via `TokenVerifier` (`staticTokenVerifier` + HS256 `hmacVerifier`; `ApprovalActor.tenantId`
required; the `tenants` registry is the allocation authority — `RESERVED_FOR_ALLOCATION`
denied at provisioning (infra slugs + `system` + `default`), `RESERVED_TENANT_IDS`
(`system`) denied at token verification AND re-refused by `createTenantResolver`
before any store binds — custom verifiers bypass `toApprovalActor`);
the public demo (`showcase/demo-auth.ts`: OAuth → ephemeral `dm*` tenant + four-role
JWT set, atomic per-tenant + global-daily run caps, kill switch in the AUTH middleware, two
cron expressions so sweep/purge never share an invocation; providers behind the `OAuthProvider`
seam — `googleProvider` is the LAUNCH provider, `githubProvider` the env-selected fallback;
one mounts per deployment, Google wins when both are configured, and the SPA reads the
provider name from `/auth/config`); subdomain↔tenant cross-check for
client-per-subdomain hosts. The deployed SPA derives identity from the server's `/workflows`
actor echo (the fail-open client actor table is gone) and its production bundle is proven
demo-token-free at build time.

## Live demo deployment (anchorage.proofoftech.org)

The showcase deploys to **`anchorage.proofoftech.org`** — a Workers custom
domain on the proofoftech.org zone, with `workers_dev: false` so it is the
ONLY public origin (the Google OAuth callback is registered for exactly
`https://anchorage.proofoftech.org/auth/google/callback`; a second origin
would break sign-in and undermine noindex). **TEMPORARY: `app/index.html`
carries a `<meta name="robots" content="noindex" />` so the live demo stays
out of search indexes pre-announce — REMOVE that meta (and this reminder)
when the demo should be indexable.** Google OAuth is the launch provider
(`GOOGLE_CLIENT_ID` var + `GOOGLE_CLIENT_SECRET` secret; the Google consent
screen must be "In production" — Testing status only admits listed test
users); GitHub stays a config-only fallback.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `README.md` | Project overview, architecture diagram, package map, quick start | Onboarding, understanding project purpose |
| `CONTRIBUTING.md` | Setup, workflow, and PR guidelines | Preparing a PR, first-time setup |
| `LICENSE` | Apache-2.0 license text | Licensing questions |
| `SECURITY.md` | Vulnerability reporting (GitHub private advisories) + scope | Reporting or triaging a security issue |
| `.github/workflows/ci.yml` | CI — verification gate + `spike:verify` on push/PR to `main` | Changing CI, debugging a failed check |
| `package.json` | Root workspace manifest and `-r` scripts (build/dev/test/lint/typecheck) | Adding dependencies, modifying workspace scripts |
| `pnpm-workspace.yaml` | pnpm workspace package globs | Adding or removing workspace packages |
| `pnpm-lock.yaml` | Resolved dependency lockfile (generated) | Never edit by hand — regenerated by `pnpm install` |
| `tsconfig.base.json` | Shared TypeScript compiler config | Modifying compiler options across all packages |
| `biome.json` | Biome lint/format configuration | Changing lint or format rules |
| `.gitignore` | Ignored paths (build output, `.wrangler/`, `.codebase-memory/`, DB/log files) | Adding generated artifacts that must not be committed |

## Subdirectories

Each subdirectory has its own `CLAUDE.md` with a file-level index.

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `packages/` | The two implementation packages — `breakwater` (safety middleware) and `flowsafe` (approval UX + DO runner) | Implementing or modifying any shipped code |
| `docs/` | Architecture, design, security, and operations docs, plus illustrative TS workflow examples | Understanding design rationale, the threat model, or the connector/policy interfaces |

## Architecture

Anchorage is a TypeScript monorepo (pnpm workspaces) that layers enterprise
safety and approval controls on top of Mastra. It does NOT build a custom
runtime — Mastra provides workflows, agents, memory, RAG, and observability.

### Packages

| Package | Purpose | Mastra relationship |
|---------|---------|-------------------|
| **breakwater** | Safety middleware | Plugs into Mastra as processors (`@mastra/core/processors`) + wrappers |
| **flowsafe** | Approval UX + durable execution | Plugs into Mastra suspend/resume events; wraps workflows for DO |

## Key Design Decisions

- Built ON Mastra, not competing with it
- TypeScript throughout, pnpm workspaces
- breakwater ships as Mastra processors + tool/workflow wrappers — `@proofoftech/breakwater`
- flowsafe ships as a standalone service + React UI — `@proofoftech/flowsafe`
- Cloudflare-native execution via Durable Objects (init()-based import-swap,
  the same mechanism `@mastra/inngest` and the experimental `@mastra/temporal` use)
- Open-source RBAC and audit logging, shipped as Mastra middleware
- Connector approval is grant-only: the requestContext grant
  (`breakwater.approvedConnectors`) is the sole approval token, enforced on
  every path. Never re-add a `context.agent` skip to the write gate — an
  agent-shaped context is forwardable into nested/direct calls, and Mastra
  strips the pure `{approved: true}` resume before invoking the tool, so no
  in-band native-approval signal exists at execute time. Deliberate
  consequence (fail closed): an agent run approved through bare Mastra UX
  without grant-minting is denied by the wrapper; whatever handles the
  approval must mint the grant into the requestContext the resumed call
  executes under. Implemented (Phase 3): the flowsafe approval-api mints by
  DERIVATION, not transport — `approvalGrantProvider(store)` plugs into
  `RunnerRuntime.requestContextForRun` and recomputes the grant list from
  APPROVED approval records on every start/resume, so grants never cross an
  HTTP body and the DO's public resume route stays grant-free. Tenant-isolated
  (INV-2): `approvalGrantProvider` takes a `TenantBoundApprovalStore`
  (`factory.forTenant(tenantId)`); a `SystemApprovalStore` is not
  type-assignable, so derivation can only ever read one tenant's APPROVED
  records. The tenant is carried by the server-minted runId
  (`${tenantId}_${uuid}`, INV-1), never chosen by a client. Grants are
  SUSPENSION-SCOPED: the runtime passes the resumed step and its current
  suspension's `(suspendedAt, resumeCount)` fingerprint to the provider, and a
  step-keyed approval mints preferentially by EXACT MATCH — the creating
  bridge captures both from the snapshot/ledger into the record
  (`ApprovalRecord.{suspendedAt,resumeCount}`, from
  `RunSummary.{suspendedAt,resumeCount}`), and both must equal the resumed leg's
  values. `suspendedAt` comes from the core clock; `resumeCount` is the
  runtime-owned monotonic per-(run,step) resume ordinal (no clock), so the
  binding is clock-free. `resumeCount` is the categorical tie-breaker —
  undefined on a step's first suspension, `1,2,…` on re-suspensions, incremented
  by the runtime on EVERY resume regardless of payload — so a spent
  first-suspension approval never mints into a re-suspension even when the two
  `suspendedAt` stamps collide within a millisecond (possible only on the
  synchronous in-process path; production's HTTP+D1 round-trips keep them seconds
  apart), and a no-payload re-suspension (which Mastra leaves without a
  `resumedAt`) stays distinguishable. Because the ordinal strictly increments it
  never collides, so this supersedes the earlier `(suspendedAt, resumedAt)`
  binding and closes its deep-chain (3+ re-suspension) residual; `resumedAt` is
  kept as informational audit metadata only. Records created without the capture
  fall back to decided-strictly-after-suspension, correct on same-clock
  topologies only (an in-memory ledger reset across a DO restart also degrades
  to this fail-closed re-deny, never a leak). Run-scope is EXPLICIT: a step-less
  record is a run-wide standing grant only when it carries `runScoped: true`, and
  mints nothing otherwise (absent-field-implies-privilege was an inverted
  default) — so approving a connector at one gate never
  unlocks it at another gate, and a re-suspension of the same step spends the
  earlier approval. The queue's HTTP create route is off by default
  (`createApprovalRouter`'s `allowCreate`) and can never author capability: it
  400s on any body naming a `TCB_ONLY_CREATE_FIELDS` member (`connectors`,
  `stepPath`, `suspendedAt`, `resumedAt`, `resumeCount`, `runScoped`,
  `requestedBy`) and forces `requestedBy` to the authenticated actor, so no
  client can name the connectors a decision mints, pick the leg it mints on, or
  disarm the SoD check by spoofing the requester. Because Mastra merges resume-provided context
  OVER persisted
  context (pinned in runtime.test.ts; omission does not revoke), the
  provider returns the grant key on EVERY leg — empty when nothing applies —
  so each leg's overwrite retires stale grants. Grant-minting code is inside
  the trust boundary by definition: the grant is a capability token, so
  requestContext must never be populated from client input, model output,
  or tool results (`security-threat-model.md`, trust boundary 6).
  Separation of duties: `decide()` denies the requester deciding their own
  request by default (`allowSelfDecision` is the explicit opt-out).
  Mastra-native `requireApproval` stays compiled purely as the agent pause
  UX — a per-call predicate that exempts dry-run requests (a simulation
  never reaches a side effect; runtime paths that evaluate it without a
  context stay fail-closed).

### What NOT to build

- YAML workflow DSL — Mastra's code-first API won
- Custom runtime — Mastra's engine is production-hardened
- Custom state store — Mastra has 13+ storage adapters
- Multi-agent orchestration — Mastra's Supervisor pattern covers it
- Visual debugger/Studio — Mastra Studio exists

## When Making Changes

- Any new code goes under `packages/breakwater/` or `packages/flowsafe/`
- breakwater code must work as Mastra middleware (no custom runtime)
- flowsafe code must target Cloudflare Workers/DO for the DO runner
- **INV-1** — runIds are server-minted `${tenantId}_${uuid}`.
  `RunnerRuntime.start` *requires* `options.runId`; never re-add the
  `?? crypto.randomUUID()` fallback. `createRunRouter` 400s a client-supplied
  `body.runId`.
- **INV-2** — approval stores are tenant-bound. Obtain via
  `D1ApprovalStoreFactory` / `InMemoryApprovalStoreFactory` `.forTenant()`.
  `D1ApprovalStore` is deliberately not exported. `sweepSLA` is a cron-only
  free function over `SystemApprovalStore`.
- **INV-3** — `tenantId` matches `^[a-z0-9]{3,32}$`; every verifier validates
  it and drops entries that fail. `'system'` is additionally rejected at token
  verification AND re-refused by `createTenantResolver` before a store binds
  or a runId mints (`RESERVED_TENANT_IDS`, defined in
  `do-runner/path-safe-id.ts` — the TCB's own audit identity; the resolver
  belt exists because a custom `TokenVerifier` or hand-built actor map never
  crosses `toApprovalActor`); the routing/allocation slugs
  (`RESERVED_FOR_ALLOCATION`, incl. `default`) are deliberately NOT enforced
  at either of those points — re-conflating the two lists would 401 a
  single-tenant host named `api` or `default`.
