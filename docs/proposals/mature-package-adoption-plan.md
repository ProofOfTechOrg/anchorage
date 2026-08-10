# Proposal: mature package adoption plan

> Status: proposal. No package substitution in this plan is implemented or supported behavior. Every dependency decision requires the package-first design check in the root CLAUDE.md and explicit user direction before implementation.

Content type: implementation plan

## TL;DR

The repository hand-rolls several infrastructure concerns that mature packages or first-party Cloudflare tooling can own more safely. Adopt nine substitutions in four isolated phases, subject to a fresh package review and user approval for each phase:

1. Use jose for JWT signing and verification, an Octokit webhook package for GitHub signature verification when its text boundary proves byte-equivalent, and openid-client for standards-level OAuth/OIDC mechanics.
2. Use Cloudflare's Vitest integration, Wrangler test harness, and Vite plugin for real Workers, D1, Durable Object, and WebSocket behavior.
3. Introduce Hono and runtime Zod schemas at selected HTTP boundaries, then migrate additional routers only after compatibility tests prove the existing composable contract.
4. Use unified/remark, Biome plus dependency-cruiser, and publint plus Are the Types Wrong for repository tooling, provided the user accepts changing the current zero-dependency policy for root scripts.

Keep FlowSafe and Breakwater domain logic custom. Approval state transitions, capability grants, ownership checks, D1 compare-and-swap statements, schedules, reconnect policy, audit semantics, bounded tails, and content scanning encode repository-specific invariants that general packages would obscure rather than replace.

Do not install all packages in one change. Each phase must delete enough custom machinery to justify its dependency cost, preserve the named security and API invariants, pass focused and full verification, and complete the repository's three independent review lanes.

## Baseline and audit scope

This plan was written on 2026-08-10 against the following baseline:

| Item | Audited value |
| --- | --- |
| Branch | single-tenant-isolation |
| HEAD | 83a0a821407e024b815a0be0b2364c685ecdd07b |
| Refreshed target | origin/dev at 83a0a821407e024b815a0be0b2364c685ecdd07b |
| Node.js | v22.22.0 |
| pnpm | 10.34.4 |
| Workspace package manager | pnpm@10.34.4 |

The audit covered both committed code and the active working-tree changes. At the final audit snapshot, the worktree had 269 status entries and the tracked diff reported 251 files changed, 12,780 insertions, and 13,613 deletions. Those changes belong to the user and continued to move during the audit. An implementing session must refresh origin, inspect status, and re-read every target before editing. It must not assume that a symbol or snippet remains unchanged.

All line numbers are intentionally omitted. Every edit target is identified by file and symbol, with current-state excerpts in [Appendix: current-state anchors](#appendix-current-state-anchors). The excerpts are as of the baseline plus the 2026-08-10 working-tree snapshot and must be re-confirmed read-before-edit.

The audit classified findings by origin:

- Current changes introduced or substantially expanded custom JWT tickets, OAuth behavior, request-body parsing, Worker routes, provisioning scripts, and showcase development emulation.
- Baseline code already contained the D1 facsimile, GitHub webhook verifier, Markdown parser, import-graph scanners, package publication checks, and subprocess wrapper.
- The current changes increased reliance on some baseline utilities, especially the D1 facsimile and HTTP routing patterns, so the implementation plan treats both layers as one architecture.

## Package selection gate

No package named below is pre-approved. Before each phase, the implementing agent must present the current package evidence and wait for the user's decision. The review must use current registry metadata and primary documentation rather than this plan's 2026-08-10 snapshot.

For each candidate, record:

- the package name, exact proposed version, license, release date, release cadence, maintainer activity, security history, and published provenance;
- Node.js, Cloudflare Workers, browser, React, TypeScript, ESM, and package-export compatibility for the paths that will import it;
- runtime versus development-only placement and the resulting consumer, bundle, install, and cold-start cost;
- the custom code and tests that the package will delete;
- the repository-specific adapter that will remain;
- rejected alternatives and why they fit less well;
- migration and rollback boundaries;
- the user's approval or rejection.

The workspace already sets minimumReleaseAge to 10,080 minutes in [pnpm-workspace.yaml](../../pnpm-workspace.yaml). Preserve that guard. Do not bypass it for these packages without a separate user decision. Existing exclusions for first-party Cloudflare packages do not waive the maturity, compatibility, license, or security review.

Do not add an indirect import and rely on another workspace package to provide it. Each package that imports a runtime or development dependency must declare it in its own manifest.

## Findings and decisions

| Priority | Area | Current custom responsibility | Candidate | Plan verdict |
| --- | --- | --- | --- | --- |
| 1 | JWT and signed stream tickets | Compact serialization, base64url, HMAC, algorithm selection, claim checks, signing | [jose](https://github.com/panva/jose) | Adopt first, preserving FlowSafe claim validation and verifier seams |
| 2 | Cloudflare runtime tests | D1-shaped SQLite adapters and partial binding doubles | [Cloudflare Vitest integration](https://developers.cloudflare.com/workers/testing/vitest-integration/) and [Wrangler test harness](https://developers.cloudflare.com/workers/testing/test-harness/get-started/) | Adopt for fidelity tests; retain Node tests only when they make no runtime-fidelity claim |
| 3 | Showcase local runtime | Connect-to-Fetch bridge, in-memory Durable Object facsimiles, forced polling | [Cloudflare Vite plugin](https://developers.cloudflare.com/workers/vite-plugin/) | Adopt and delete the custom development Worker emulator after parity tests |
| 4 | GitHub webhook signatures | Header parsing, hex decoder, WebCrypto verification | [@octokit/webhooks-methods](https://github.com/octokit/webhooks-methods.js/) | Adopt only if the package's string boundary is byte-equivalent for accepted webhook bodies; otherwise fix the decoder and use Octokit types only |
| 5 | OAuth/OIDC protocol | Authorization URLs, token exchange, response casting, state HMAC | [openid-client](https://github.com/panva/openid-client) | Adopt behind the existing provider seam after a GitHub and Google compatibility spike |
| 6 | HTTP routing and validation | Path matching, methods, JSON responses, body limits, request schemas | [Hono](https://github.com/honojs/hono) and [Zod](https://zod.dev/packages/zod) | Adopt incrementally at top-level boundaries, not as a repository-wide rewrite |
| 7 | Markdown parsing | Fence, comment, link, heading, entity, and GitHub-slug parsing | [unified](https://unifiedjs.com/), [remark-parse](https://unifiedjs.com/explore/package/remark-parse/), and [github-slugger](https://github.com/Flet/github-slugger) | Adopt only if the user approves changing the zero-dependency root-script policy |
| 8 | Import boundaries | Regex import scanners and a custom TypeScript graph walker | [Biome restricted imports](https://biomejs.dev/linter/rules/no-restricted-imports/) plus [dependency-cruiser](https://github.com/sverweij/dependency-cruiser) | Adopt, retaining positive controls for every architecture rule |
| 9 | Published package validation | Tar extraction, manifest/export/type checks, temporary consumer setup | [publint](https://publint.dev/docs/) plus [Are the Types Wrong](https://github.com/arethetypeswrong/arethetypeswrong.github.io) | Adopt for standard publication checks; retain repository-specific runtime and security probes |

Three additional packages are conditional rather than recommended now:

- [Execa](https://github.com/sindresorhus/execa) can replace Node-only subprocess boilerplate, but only after proving that it cannot enter Worker-facing exports.
- [TanStack Query](https://tanstack.com/query/v5/docs/framework/react/guides/polling) can own REST query, mutation, cache, and polling state in the approval UI, but it cannot own ticket refresh, stream ordering, heartbeats, presence, or conflict semantics. It also changes the public React integration surface.
- A CLI framework such as Commander is not justified by the current small provisioning parser. Reconsider it only when the CLI gains subcommands or shared option behavior.

## Intended architecture

The package boundary should separate protocol and framework mechanics from domain invariants:

| Package-owned mechanics | Repository-owned invariants |
| --- | --- |
| JWT compact serialization, cryptographic verification, registered claim checks | Approval actor validation, roles, key lookup policy, ticket channel and path constraints |
| OAuth authorization and token protocol | Session storage, subject naming, actor roles, run budget, login-CSRF binding |
| Worker runtime, D1, Durable Objects, WebSocket upgrades | Topology, resource ownership, grants, state transitions, audit ordering |
| Route matching, body limits, structural schemas | Authentication order, authorization order, error envelope, composable null contract |
| Markdown AST and GitHub heading slugs | Repository link, navigation, proposal, API-doc, and publication policies |
| TypeScript dependency graph and direct import rules | Allowed package layers and positive-control fixtures |
| Standard package manifest and type-resolution checks | Browser-clean exports, runtime behavior, executable assertions, security boundaries |

An implementation that adds a package without deleting its corresponding generic machinery fails this architecture. An implementation that moves domain invariants into a generic library also fails it.

## Phase 0: obtain package decisions

Group the approval request into four independent decisions:

1. Security protocols: jose, @octokit/webhooks-methods, optional Octokit webhook types, and openid-client.
2. Cloudflare fidelity: @cloudflare/vitest-pool-workers, Wrangler's test harness API, and @cloudflare/vite-plugin.
3. HTTP boundaries: Hono and runtime Zod.
4. Repository tooling: unified/remark/github-slugger, dependency-cruiser, publint, and @arethetypeswrong/cli.

For each group, present the package-selection record described above and a no-new-dependency alternative. Approval for one group does not authorize another. A rejected group leaves the corresponding code unchanged unless the user separately requests a narrow bug fix.

## Phase 1: replace protocol mechanics

### 1A. JWTs and stream tickets with jose

Current evidence: [A1](#a1-manual-jwt-verification-and-signing) and [A2](#a2-custom-stream-ticket-format).

Files in scope:

- [packages/flowsafe/src/host-kit/verifier.ts](../../packages/flowsafe/src/host-kit/verifier.ts)
- [packages/flowsafe/src/host-kit/stream-ticket.ts](../../packages/flowsafe/src/host-kit/stream-ticket.ts)
- their direct consumers, fixtures, tests, exports, and generated API documentation
- [packages/flowsafe/package.json](../../packages/flowsafe/package.json) and [pnpm-lock.yaml](../../pnpm-lock.yaml)

Decision:

- Add jose as a FlowSafe runtime dependency because published FlowSafe code will import it.
- Preserve TokenVerifier, staticTokenVerifier, toApprovalActor, HmacVerifierOptions, injected clocks, the exact one-key fallback, and the non-URL key lookup by kid.
- Implement hmacVerifier with jwtVerify. Pin algorithms to HS256 and pass issuer, audience, and an injected currentDate. Continue to require exp and validate the domain role and subject through toApprovalActor.
- Implement mintHmacToken with SignJWT and a protected header containing alg, typ, and kid.
- Convert stream tickets from the private two-segment format to a standard signed JWT under STREAM_TICKET_SECRET. Give them a distinct typ such as flowsafe-stream-ticket+jwt and a dedicated audience so an actor JWT and stream ticket cannot cross-verify even if a deployment is misconfigured.
- Continue to validate channel, workflowId, runId, actorId, role, path safety, required field relationships, and expiry after cryptographic verification.
- Do not replace deployment-identity raw secret comparison. It is not a JWT and uses a different trust boundary.

Benefits:

- Deletes custom compact serialization, base64url decoding, HMAC key plumbing, signing, timing-safe comparison, and registered-claim interpretation.
- Centralizes algorithm pinning and claim behavior in a library maintained specifically for JOSE.
- Makes the stream ticket format inspectable by standard tools while retaining a dedicated key and purpose.
- Reduces the future cost of key rotation or an asymmetric algorithm without changing router seams.

Risks and controls:

- jose registered-claim defaults must not silently broaden clock tolerance. Set every security-relevant option explicitly and add boundary tests at exact exp and nbf times.
- Tokens minted before the stream-ticket format switch will stop verifying. Stream tickets are short-lived and not persisted, but the rollout must still update every issuer and verifier atomically.
- Do not expose raw jose payloads as ApprovalActor values. The trusted conversion remains toApprovalActor.
- The package's ESM and Cloudflare Workers compatibility must be verified against the exact selected version before approval.

Done for 1A means the manual JWT and ticket primitives are gone, all malformed-token and cross-purpose tests still fail closed, package exports remain browser and Worker compatible, and the focused plus full gates pass.

### 1B. GitHub signatures with Octokit

Current evidence: [A3](#a3-github-hex-decoder).

Files in scope:

- [packages/flowsafe/src/signal-providers/github-provider.ts](../../packages/flowsafe/src/signal-providers/github-provider.ts)
- [packages/flowsafe/src/signal-providers/github-provider.test.ts](../../packages/flowsafe/src/signal-providers/github-provider.test.ts)
- [packages/flowsafe/src/signal-providers/webhook-route.ts](../../packages/flowsafe/src/signal-providers/webhook-route.ts)
- [packages/flowsafe/src/signal-providers/webhook-ingestion.integration.test.ts](../../packages/flowsafe/src/signal-providers/webhook-ingestion.integration.test.ts)

The current hex decoder has a real validation defect. Number.parseInt accepts a valid leading nibble followed by invalid text, so values such as 0g and aZ can decode to bytes instead of failing. This does not create an HMAC forgery, but it makes the signature representation non-canonical and violates the documented malformed-input contract.

Decision:

- First add a fixture matrix that signs raw UTF-8 JSON, non-ASCII text, a BOM, invalid UTF-8, empty input, upper- and lowercase canonical hex, wrong algorithms, wrong lengths, and invalid hex in either nibble.
- Spike @octokit/webhooks-methods verify against the exact raw request representation. Its API accepts a payload string, while the current boundary verifies Uint8Array before parsing.
- Adopt the package only if fatal UTF-8 decoding followed by the package's encoding verifies exactly the same accepted byte set. Preserve the verify-before-JSON.parse order.
- If byte equivalence is not provable, retain WebCrypto and replace hexToBytes with an exact 64-hex-character validation followed by decoding. In that outcome, optionally use @octokit/openapi-webhooks-types for event typing without leaking those types into the public API.
- Preserve createWebhookSignalProvider, subscription matching, notification shaping, secret configuration, and the current failure envelope.

Benefits:

- A successful adoption removes signature-header format parsing and cryptographic details from application code.
- Octokit tracks GitHub's webhook conventions and provides a shared vocabulary with GitHub's event types.
- The pre-swap fixture matrix prevents the package boundary from changing raw-body security semantics.

Done for 1B means every malformed representation fails, valid GitHub fixtures pass, invalid UTF-8 never reaches JSON parsing, subscription and notification behavior is unchanged, and the selected package path has documented byte equivalence. If the package fails that condition, the plan's correct result is the narrow decoder fix, not forced adoption.

### 1C. OAuth/OIDC with openid-client

Current evidence: [A4](#a4-manual-oauth-protocol).

Files in scope:

- [packages/showcase/worker/demo-auth.ts](../../packages/showcase/worker/demo-auth.ts)
- [packages/showcase/worker/demo-auth.test.ts](../../packages/showcase/worker/demo-auth.test.ts)
- [packages/showcase/worker/worker.fetch.e2e.test.ts](../../packages/showcase/worker/worker.fetch.e2e.test.ts)
- [packages/showcase/package.json](../../packages/showcase/package.json)

Decision:

- Add openid-client only to the private showcase runtime after a Workers compatibility spike.
- Use it for authorization-code URL construction, callback parameter validation, code exchange, token response validation, issuer configuration, and Google UserInfo or ID-token behavior.
- Test whether a manually configured OAuth 2 authorization server works correctly with GitHub's non-OIDC endpoints. If it does not, keep the small GitHub profile adapter and either use openid-client only for Google or present oauth4webapi as a separately reviewed alternative.
- Preserve the OAuthProvider seam so existing provider fakes remain possible.
- Preserve D1 sessions, stable provider-prefixed subjects, the four-role actor mapping, run budgets, HttpOnly and SameSite cookie behavior, and login-CSRF binding.
- Decide separately whether to replace signed state with high-entropy opaque state stored in the login cookie/session. Do not combine that semantic change with the protocol-client swap unless tests establish identical expiry, one-time-use, and replay behavior.

Rejected options:

- Arctic was deprecated in July 2026 and does not meet the mature, supported package criterion.
- @hono/oauth-providers is not the first choice for this security boundary because the audit did not establish the same stability and protocol depth as openid-client.

Benefits:

- Removes hand-built authorization URLs, token POSTs, token response casts, and provider-specific protocol assumptions.
- Gains standards-level validation and clearer failure modes.
- Keeps showcase-specific identity and authorization policy in the repository.

Done for 1C means Google and GitHub success, denial, malformed callback, invalid state, provider error, token error, and profile error paths have parity tests; secrets never enter logs or URLs beyond required protocol fields; and no session or role behavior changes.

## Phase 2: replace Cloudflare runtime emulation

### 2A. Test against workerd and real bindings

Current evidence: [A5](#a5-d1-shaped-sqlite-facsimile).

Files in scope include:

- [packages/flowsafe/test-support/sqlite.ts](../../packages/flowsafe/test-support/sqlite.ts)
- FlowSafe tests that import d1DatabaseLike
- [packages/breakwater/src/connector-sdk/d1-idempotency-store.test.ts](../../packages/breakwater/src/connector-sdk/d1-idempotency-store.test.ts)
- [packages/breakwater/src/connector-sdk/d1-rate-limit-store.test.ts](../../packages/breakwater/src/connector-sdk/d1-rate-limit-store.test.ts)
- [packages/showcase/worker/worker.fetch.e2e.test.ts](../../packages/showcase/worker/worker.fetch.e2e.test.ts)
- Vitest workspace configuration and package Wrangler configuration

Decision:

- Add @cloudflare/vitest-pool-workers as a development dependency in every workspace package whose Vitest project imports it, or centralize it at the root only if the workspace configuration is the sole importer.
- Use the current cloudflareTest plugin API with each package's real wrangler configuration. Do not copy a binding list into a test-only facsimile.
- Create separate Workers and Node Vitest projects. Keep pure domain, parsing, and deterministic state-machine tests in Node. Move tests that assert D1 envelopes, transaction behavior, Durable Objects, R2, WebSockets, or Worker globals into workerd.
- Use Wrangler's createTestHarness for full Worker fetch tests where the Vitest worker pool cannot express deployment-level behavior.
- Delete the full D1Database-shaped adapter and the package copies after all fidelity claims move to actual D1. A small SQLite fake may remain only for explicitly named unit tests that do not claim D1 compatibility.
- Preserve concurrency tests. Replace serialized JavaScript queues with concurrent calls against real bindings so compare-and-swap and lease behavior is exercised where it runs.

The 2026-08-10 first-party configuration shape to re-check at implementation time is:

~~~ts
import { cloudflareTest } from '@cloudflare/vitest-pool-workers';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: './wrangler.jsonc' },
    }),
  ],
});
~~~

Benefits:

- Tests observe actual Worker globals, binding envelopes, Durable Object storage, WebSocket upgrade behavior, and D1 semantics.
- Removes a large, duplicated compatibility layer whose behavior can drift from Cloudflare.
- Makes concurrency results meaningful instead of serializing them inside the fake.

Risks and controls:

- Workerd tests cost more and isolate differently from Node tests. Keep the project split and run the smallest correct environment for each test.
- Existing spike configurations and migrations are real architecture. Reuse them rather than inventing a parallel test schema.
- Ensure each test uses isolated storage and deterministic migrations. A passing test must not depend on execution order.

Done for 2A means no test claims D1, Durable Object, R2, WebSocket, or Worker fidelity through a hand-built object; concurrent storage tests run against real bindings; Node tests remain fast; and all package suites pass in a clean process.

### 2B. Run showcase development through Cloudflare's Vite plugin

Current evidence: [A6](#a6-custom-vite-worker-emulator).

Files in scope:

- [packages/showcase/run-api-dev-plugin.ts](../../packages/showcase/run-api-dev-plugin.ts)
- [packages/showcase/src/run-api-dev-plugin.test.ts](../../packages/showcase/src/run-api-dev-plugin.test.ts)
- [packages/showcase/vite.config.ts](../../packages/showcase/vite.config.ts)
- [packages/showcase/wrangler.jsonc](../../packages/showcase/wrangler.jsonc)
- showcase development documentation and scripts

Decision:

- Add @cloudflare/vite-plugin as a showcase development dependency.
- Configure cloudflare() alongside the existing React plugin and reuse wrangler.jsonc.
- Preserve existing aliases, SPA asset handling, metadata assertions, and production build behavior.
- Route local API traffic through the actual Worker and bindings. Prove hub and run WebSocket upgrades before removing the polling-only development path.
- Delete runApiDevPlugin and its in-memory hub/runner fakes after equivalent tests run through the Worker environment.

The 2026-08-10 first-party configuration shape to re-check is:

~~~ts
import { cloudflare } from '@cloudflare/vite-plugin';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react(), cloudflare()],
});
~~~

Benefits:

- Local development and deployment execute the same Worker entry point and binding graph.
- WebSockets work in development, so streaming behavior no longer silently degrades to polling.
- Deletes the Connect request bridge and in-memory Durable Object facsimiles.

Done for 2B means pnpm --filter showcase dev serves the SPA and Worker together, both WebSocket channels upgrade and deliver events, fallback polling still works when a stream is deliberately unavailable, and the production build remains clean.

## Phase 3: introduce Hono and runtime Zod incrementally

Current evidence: [A7](#a7-custom-body-reader) and [A8](#a8-manual-router-and-schema).

The audit found manual route or request parsing in at least these production modules:

- agent-starter/src/worker.ts
- flowsafe agent-host/router and thread-host
- approval-api/router
- background-tasks/routes
- do-runner/durable-object and hub-do
- goals/objective-routes
- host-kit/flowsafe-worker, run-router, and stream-router
- schedules/router
- signal-providers/host-do and webhook-route
- signals/router and thread-do-routes
- showcase/worker/demo-auth

Decision:

- Add Hono to each package that imports it directly. Move Zod from a FlowSafe development dependency to a runtime dependency when published FlowSafe code begins importing runtime schemas.
- Start with top-level assembly boundaries: createFlowsafeWorker, agent-starter, and showcase. Do not rewrite every public router in one change.
- Preserve each reusable router's Promise<Response | null> contract. For a route family, perform an exact path-family check before calling Hono so unrelated paths still return null; inside the claimed family, retain the existing 404 behavior.
- Preserve method-check, authentication, authorization, resource-existence, resource-ownership, body-read, and audit ordering. Snapshot response status, headers, and JSON envelopes before migrating a route.
- Use Hono bodyLimit for ordinary JSON requests. Retain a narrow raw-byte reader for webhook signatures, audit bytes, fatal UTF-8 requirements, and any route where exact body bytes are part of the security contract.
- Use strict Zod object schemas at external JSON boundaries. Map Zod issues to the existing stable error messages rather than exposing library error structures.
- Do not adopt Hono RPC types in the first migration. They could change public declaration output and consumer TypeScript requirements.

Representative target adapter:

~~~ts
const claimsAgentsPath = (pathname: string) =>
  pathname === '/agents' || pathname.startsWith('/agents/');

return async (request) => {
  if (!claimsAgentsPath(new URL(request.url).pathname)) return null;
  return app.fetch(request);
};
~~~

Benefits:

- Consolidates route matching, path parameter decoding, method dispatch, middleware, body limits, and structural validation.
- Removes repeated JSON response helpers and unsafe boundary casts.
- Gives future endpoints one established composition pattern.

Risks and controls:

- Hono's unmatched default is a Response, while current subrouters return null outside their path family. The adapter and contract tests are mandatory.
- Middleware order is security behavior. A more concise route is not equivalent if it authenticates, resolves resources, reads a body, or emits audit data in a different order.
- Hono and Zod become public runtime dependencies where imported from published modules. Measure package and declaration impact before approval.

Done for Phase 3 means one route family migrates at a time, all previous request/response matrices pass, outside-family requests still return null, raw webhook bodies remain byte-exact, public declaration and React 18 probes pass, and deleted parsing code exceeds the adapter added around Hono.

## Phase 4: replace repository tooling parsers

### 4A. Parse Markdown with unified and remark

Current evidence: [A9](#a9-custom-markdown-parser).

Files in scope:

- [scripts/docs-check.mjs](../../scripts/docs-check.mjs)
- [scripts/docs-check.test.mjs](../../scripts/docs-check.test.mjs)
- scripts/CLAUDE.md
- root package manifest and lockfile

The current scripts/CLAUDE.md explicitly describes these checks as zero-dependency. A parser package is therefore an architecture change, not a mechanical cleanup.

Decision:

- Ask the user to choose between preserving zero-dependency root checks and adopting a real Markdown AST.
- If package adoption wins, amend scripts/CLAUDE.md in the same change so the repository instructions match the architecture.
- Use unified, remark-parse, remark-gfm, unist-util-visit, mdast-util-to-string, and github-slugger for Markdown structure and GitHub-compatible heading IDs.
- Add remark-rehype and rehype-raw only if fixtures prove that raw HTML link extraction requires them. Avoid adding an HTML pipeline speculatively.
- Preserve repository-specific policies for local targets, fragments, public navigation, proposal status, generated API documentation, publication content, and external-link mode.
- Keep fixture tests for duplicate headings, punctuation, entities, fenced code, inline code, comments, reference links, images, raw HTML, Unicode, and malformed Markdown.

Benefits:

- Replaces a partial Markdown parser with an AST maintained for the grammar.
- Uses GitHub's slug behavior instead of a local approximation.
- Lets repository code express policy against nodes rather than syntax edge cases.

If the user keeps the zero-dependency rule, retain the current parser and limit changes to proven defects with regression fixtures. Do not install packages while leaving the instruction contradictory.

### 4B. Enforce import architecture with Biome and dependency-cruiser

Current evidence: [A10](#a10-custom-import-graph-scanners).

Files in scope:

- [packages/flowsafe/src/agent-host/import-isolation.test.ts](../../packages/flowsafe/src/agent-host/import-isolation.test.ts)
- [packages/flowsafe/src/host-kit/barrel-isolation.test.ts](../../packages/flowsafe/src/host-kit/barrel-isolation.test.ts)
- packages/agent-starter/scripts/check-public-imports.mjs
- [biome.json](../../biome.json)
- a dependency-cruiser configuration in the established root configuration location

Decision:

- Use the existing Biome noRestrictedImports configuration for direct forbidden imports with clear local diagnostics.
- Add dependency-cruiser for transitive reachability, cycles, package-layer rules, and TypeScript-aware resolution.
- Delete regex parsing and custom .js-to-.ts/.tsx/index resolution once every rule has an equivalent declarative check.
- Preserve a positive-control fixture for every rule. Each test must demonstrate that a known forbidden edge fails, so a resolver misconfiguration cannot make the suite pass vacuously.

Benefits:

- One TypeScript-aware graph replaces duplicated source-text scanners.
- Rules become reviewable architecture declarations rather than parsing code.
- Resolution behavior follows package exports and TypeScript paths more closely.

Done for 4B means direct and transitive violations produce deterministic failures, positive controls fail for the intended reason, allowed package directions remain documented, and no custom import syntax parser remains.

### 4C. Validate packages with publint and Are the Types Wrong

Current evidence: [A11](#a11-custom-packed-consumer-checks).

Files in scope:

- [packages/breakwater/scripts/packed-consumer-test.mjs](../../packages/breakwater/scripts/packed-consumer-test.mjs)
- [packages/flowsafe/scripts/agent-host-pack-test.mjs](../../packages/flowsafe/scripts/agent-host-pack-test.mjs)
- [packages/flowsafe/scripts/signals-client-pack-test.mjs](../../packages/flowsafe/scripts/signals-client-pack-test.mjs)
- [packages/flowsafe/scripts/provisioning-pack-test.mjs](../../packages/flowsafe/scripts/provisioning-pack-test.mjs)
- package and root scripts that orchestrate packed checks

Decision:

- Add publint and @arethetypeswrong/cli as pinned development-only tools after reviewing their pre-1.0 API and release stability.
- Run them against each actual pnpm pack artifact.
- Delete custom checks for standard manifest fields, export maps, declaration resolution, and common module-resolution modes once tool coverage is demonstrated.
- Retain project-specific probes: Worker/browser-clean imports, no secret or Node-only leakage, runtime execution, executable permissions where relevant, React compatibility, and security-boundary assertions.
- Keep a real temporary consumer TypeScript compile only for behavior the tools do not cover.

Benefits:

- Broader package-resolution coverage with less bespoke tar and manifest code.
- Standard diagnostics that package maintainers recognize.
- Custom scripts focus on Anchorage guarantees instead of reimplementing package ecosystem rules.

Done for 4C means all published entry points pass both tools, retained custom probes still catch seeded failures, pack contents stay minimal, and every existing package consumer scenario still runs.

## Conditional opportunities

### Execa

The default exec path in [packages/breakwater/src/agent-cli/index.ts](../../packages/breakwater/src/agent-cli/index.ts) manually coordinates spawn, output, timeout, termination, and error conversion. Execa can reduce Node subprocess boilerplate, but Breakwater also publishes Worker-compatible code.

Use Execa first in Node-only repository scripts if several wrappers converge on the same behavior. Consider it in agent-cli only after a package-boundary test proves that static imports cannot enter Worker-facing exports. Preserve bounded output tails, timeout escalation, AbortSignal behavior, exit metadata, and redaction. Until that proof exists, the custom wrapper is the safer narrow boundary.

### TanStack Query

[packages/flowsafe/src/approval-ui/use-approval-dashboard.ts](../../packages/flowsafe/src/approval-ui/use-approval-dashboard.ts) owns REST fetch state, mutation state, interval polling, stream reconciliation, expiring tickets, heartbeat, presence, and conflicts.

TanStack Query is a fit for REST query keys, caching, mutations, invalidation, and fallback polling. It is not a replacement for ordered stream reconciliation or ticket lifecycle. Adoption would add a public peer or provider requirement to the approval UI, so present that API decision separately. Do not mix it into the infrastructure phases.

### CLI framework

[packages/flowsafe/scripts/seed-deployment-identity.mjs](../../packages/flowsafe/scripts/seed-deployment-identity.mjs) has a small argument parser. Commander, yargs, or citty would currently add more surface than they delete. Reopen the decision when the provisioning CLI has subcommands, reusable option groups, shell completions, or generated help that the repository would otherwise implement itself.

## Packages considered and rejected

### Drizzle ORM

Do not adopt Drizzle in this plan. As of 2026-08-10, the official [Cloudflare D1 guide](https://orm.drizzle.team/docs/sqlite/connect-cloudflare-d1) used release-candidate packages, and Cloudflare listed Drizzle under [community projects](https://developers.cloudflare.com/d1/reference/community-projects/). That does not meet this audit's strict mature-stable criterion.

More importantly, an ORM would not replace the repository's core D1 invariants. Statements such as insert-on-conflict reservation and state-guarded lease takeover are the concurrency protocol. Keep them visible and tested against real D1. Re-evaluate only after a stable D1 adapter exists and a spike proves equal SQL control, migration ownership, Worker compatibility, and a meaningful net deletion.

### Cron parser

Do not add one. Scheduling already reuses Mastra's ScheduleInputSchema, validateCron, and computeNextFireAt. Another parser would duplicate an existing dependency and risk semantic drift.

### Generic state machine or RBAC package

Do not replace approval transitions, separation of duties, capability grants, resource ownership, or compare-and-swap guards. These are product and threat-model semantics rather than generic mechanics.

### Generic reconnecting WebSocket package

Do not replace the current stream controller solely to obtain reconnection. The controller couples reconnects to fresh short-lived tickets, heartbeat timing, ordered event reconciliation, and polling fallback. A package would still need a substantial custom policy adapter and would not remove the hard code.

### Miscellaneous utility packages

Keep small ID, cursor, ring-buffer, bounded-tail, and content-scanning helpers custom while they remain short, invariant-specific, tested, and dependency-free. A utility package is justified only when it removes a meaningful class of edge cases or repeated machinery.

## Reuse map

Implementing sessions must extend these existing seams instead of replacing them:

| Existing seam or pattern | Path | Required reuse |
| --- | --- | --- |
| TokenVerifier and toApprovalActor | [host-kit/verifier.ts](../../packages/flowsafe/src/host-kit/verifier.ts) | Keep identity conversion and verifier injection stable |
| OAuthProvider | [showcase/worker/demo-auth.ts](../../packages/showcase/worker/demo-auth.ts) | Hide protocol-client details and retain provider fakes |
| Raw bounded bytes | [flowsafe/src/http-body.ts](../../packages/flowsafe/src/http-body.ts) | Narrow and retain for byte-sensitive security boundaries |
| Composable Response or null routers | FlowSafe router modules | Preserve outside-family fallthrough |
| Wrangler deployment and spike configs | package wrangler.jsonc files | Drive test and Vite plugins from real bindings |
| Existing Vitest workspace | root and package Vitest configuration | Add separate Node and Workers projects |
| Biome restricted imports | [biome.json](../../biome.json) | Enforce direct edges without duplicate tooling |
| Mastra schedule schemas | FlowSafe schedule modules | Continue using existing schedule validation |
| React 18 compatibility probe | FlowSafe package scripts | Run after any approval UI dependency change |
| Package consumer probes | package scripts | Retain repository-specific runtime guarantees |
| Documentation policy functions | [scripts/docs-check.mjs](../../scripts/docs-check.mjs) | Keep policy while replacing syntax parsing |
| Supply-chain release-age guard | [pnpm-workspace.yaml](../../pnpm-workspace.yaml) | Apply to every dependency selection |

## Out of scope

- Do not implement or install any package as part of this documentation change.
- Do not rewrite domain stores, SQL concurrency statements, approval transitions, grants, ownership checks, audit events, or schedule semantics.
- Do not change public route paths, response bodies, status codes, headers, authentication order, authorization order, or outside-family null behavior.
- Do not combine these phases with new product features.
- Do not expose new third-party types through public exports unless the user explicitly approves that API.
- Do not alter deployment secret comparison, content-scanner policy, event ordering, replay policy, or ticket lifetime except where a dedicated migration decision says so.
- Do not migrate all routers to Hono in one pull request.
- Do not remove project-specific package probes after adding publint or Are the Types Wrong.
- Do not change scripts/CLAUDE.md's zero-dependency rule silently.
- Do not adopt a release-candidate dependency to satisfy this plan.

## Rollout and pull request structure

Feature and fix pull requests target dev. Refresh origin before each branch decision.

Use separate pull requests:

1. Protocol security: jose first, then Octokit verification, then OAuth. These may be separate commits or separate pull requests if review size grows.
2. Cloudflare fidelity: Workers test projects and harness first, then the showcase Vite plugin. Remove emulators only after parity is proven.
3. HTTP boundary pilot: migrate one top-level route family, measure diff and package impact, then ask before expanding.
4. Repository tooling: import rules and package validators can proceed independently; the Markdown parser requires an explicit zero-dependency policy decision.

Each pull request must include:

- the approved package-selection record;
- before and after behavior matrices for risky boundaries;
- a deletion inventory showing what the package replaced;
- dependency placement and lockfile changes;
- focused tests, full verification, and three independent review-lane verdicts;
- an updated plan status if a decision changes or a package is rejected.

## Verification

### Documentation-only change that introduced this plan

Run:

~~~bash
git diff --check
pnpm docs:check
pnpm lint
~~~

Done means the proposal is indexed, all links and fragments resolve, root CLAUDE.md contains the package-first gate, Biome reports no documentation-format issue, and no runtime manifest or lockfile changed.

### Phase 1 focused verification

Run the relevant focused suites:

~~~bash
pnpm --filter @proofoftech/flowsafe test -- src/host-kit/verifier.test.ts src/host-kit/stream-ticket.test.ts src/host-kit/stream-router.test.ts
pnpm --filter @proofoftech/flowsafe test -- src/signal-providers/github-provider.test.ts src/signal-providers/webhook-route.test.ts src/signal-providers/webhook-ingestion.integration.test.ts
pnpm --filter showcase test -- worker/demo-auth.test.ts worker/worker.fetch.e2e.test.ts
pnpm --filter @proofoftech/flowsafe typecheck
pnpm --filter showcase typecheck
pnpm --filter @proofoftech/flowsafe spike:verify
~~~

Done means valid fixtures still verify, malformed and cross-purpose tokens fail closed, exact expiry boundaries are covered, raw GitHub signature behavior is proven, OAuth error paths have parity, and no secret-bearing data appears in error output.

### Phase 2 focused verification

Run:

~~~bash
pnpm --filter @proofoftech/flowsafe test
pnpm --filter @proofoftech/breakwater test -- src/connector-sdk/d1-idempotency-store.test.ts src/connector-sdk/d1-rate-limit-store.test.ts
pnpm --filter showcase test
pnpm --filter @proofoftech/flowsafe spike:verify
pnpm --filter showcase build
~~~

Also start pnpm --filter showcase dev in a bounded test session and prove both hub and run WebSocket upgrades, event delivery, reconnect, and deliberate polling fallback. Done means tests use real bindings for every fidelity claim, storage is isolated, concurrent calls are not serialized by a fake, and local development uses the deployed Worker topology.

### Phase 3 focused verification

Run:

~~~bash
pnpm --filter @proofoftech/flowsafe test -- src/agent-host/router.test.ts
pnpm --filter @proofoftech/flowsafe typecheck
pnpm --filter @proofoftech/flowsafe build
pnpm --filter @proofoftech/flowsafe typecheck:react18
pnpm --filter @proofoftech/flowsafe test:agent-host-export
pnpm --filter showcase test
pnpm --filter showcase typecheck
~~~

Add the corresponding focused command for each route family before migrating it. Done means behavior matrices are byte-for-byte stable where specified, outside-family fallthrough remains null, middleware order is covered, and published declarations do not leak unwanted Hono or Zod types.

### Phase 4 focused verification

Run:

~~~bash
pnpm docs:check:test
pnpm docs:check
pnpm lint
pnpm test:packed-breakwater
pnpm test:packed-flowsafe-agent-host
pnpm test:packed-flowsafe-provisioning
pnpm --filter @proofoftech/flowsafe test:signals-client-export
~~~

Done means Markdown fixtures cover syntax previously parsed by hand, each architecture rule has a failing positive control, both package tools pass actual tarballs, and custom consumer probes still catch repository-specific breakage.

### Full completion gate for every implementation pull request

After dependency and lockfile changes settle, run from a clean install:

~~~bash
pnpm install --frozen-lockfile
git diff --check
pnpm docs:check
pnpm docs:api
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm --filter @proofoftech/flowsafe spike:verify
pnpm --filter @proofoftech/flowsafe typecheck:react18
pnpm --filter showcase react-doctor
~~~

Run the repository's clean-code, architecture, and QA review lanes independently. Fix every substantive finding and rerun that lane. Completion requires all three lanes to return clean.

## Benefits to measure

Do not call a substitution successful because tests pass. Record:

- custom production and test lines deleted versus adapter lines added;
- number of duplicate implementations removed;
- runtime dependency and transitive dependency count;
- packed size and browser/Worker bundle impact;
- cold-start or build-time regression where relevant;
- edge-case fixtures now delegated to the package;
- number of repository-specific invariants that remain explicit;
- maintenance ownership shifted to first-party or specialist maintainers.

Reject or roll back a package if it adds a large abstraction while deleting little code, leaks types into public APIs, weakens runtime portability, hides security ordering, or still requires most of the original custom machinery.

## Appendix: current-state anchors

The following excerpts pin the audited edit targets. They are evidence, not implementation patches. Re-read each symbol against the current branch before editing.

### A1. Manual JWT verification and signing

File: [packages/flowsafe/src/host-kit/verifier.ts](../../packages/flowsafe/src/host-kit/verifier.ts)

Symbol: base64UrlDecodeBytes

~~~ts
function base64UrlDecodeBytes(segment: string): Uint8Array | undefined {
  const normalized = segment.replace(/-/g, '+').replace(/_/g, '/');
  const padded = normalized + '='.repeat((4 - (normalized.length % 4)) % 4);
  try {
    const binary = atob(padded);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    return bytes;
  } catch {
    return undefined;
  }
}
~~~

Symbol: hmacVerifier, current security sequence

~~~ts
const parts = token.split('.');
if (parts.length !== 3) return undefined;
const [headerPart, claimsPart, signaturePart] = parts as [
  string,
  string,
  string,
];

const header = base64UrlDecodeJson(headerPart) as JwtHeader | undefined;
if (header?.alg !== 'HS256') return undefined;
~~~

Symbol: mintHmacToken, current signing setup

~~~ts
const header = { alg: 'HS256', typ: 'JWT', kid: options.kid };
const claims = {
  iss: options.issuer,
  aud: options.audience,
  sub: options.actor.id,
  role: options.actor.role,
  iat: nowSeconds,
  exp: nowSeconds + options.ttlSeconds,
};
~~~

Target shape, subject to the selected jose version:

~~~ts
const { payload, protectedHeader } = await jwtVerify(token, key, {
  algorithms: ['HS256'],
  issuer: options.issuer,
  audience: options.audience,
  currentDate: new Date(now()),
});
if (protectedHeader.alg !== 'HS256') return undefined;
return toApprovalActor({ id: payload.sub, role: payload.role });
~~~

The implementation must resolve the key from the protected kid before verification and preserve the single-key fallback. It must catch verification failures at the TokenVerifier boundary and return undefined.

### A2. Custom stream ticket format

File: [packages/flowsafe/src/host-kit/stream-ticket.ts](../../packages/flowsafe/src/host-kit/stream-ticket.ts)

Symbol: mintStreamTicket

~~~ts
const payload = base64UrlEncode(encoder.encode(JSON.stringify(claims)));
const signature = await hmacSign(secret, payload);
return payload + '.' + signature;
~~~

Symbol: verifyStreamTicket

~~~ts
const parts = token.split('.');
if (parts.length !== 2) return undefined;
const [payload, signature] = parts as [string, string];

const expected = await hmacSign(secret, payload);
if (!constantTimeEqual(signature, expected)) return undefined;
~~~

Target:

~~~ts
const token = await new SignJWT(claims)
  .setProtectedHeader({
    alg: 'HS256',
    typ: 'flowsafe-stream-ticket+jwt',
  })
  .setAudience(STREAM_TICKET_AUDIENCE)
  .setExpirationTime(claims.exp)
  .sign(key);
~~~

Verification must pin the same alg, typ, audience, secret, and current date before applying the existing channel and path-safe domain checks.

### A3. GitHub hex decoder

File: [packages/flowsafe/src/signal-providers/github-provider.ts](../../packages/flowsafe/src/signal-providers/github-provider.ts)

Symbol: hexToBytes

~~~ts
function hexToBytes(hex: string): Uint8Array | undefined {
  if (hex.length === 0 || hex.length % 2 !== 0) return undefined;
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    const byte = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
    // parseInt tolerates trailing garbage; Number.isNaN catches non-hex.
    if (Number.isNaN(byte)) return undefined;
    bytes[index] = byte;
  }
  return bytes;
}
~~~

Before package adoption, add regression cases showing that both characters of every byte must be hexadecimal. The target call is conceptually:

~~~ts
return verify(secret, decodedRawBody, signatureHeader);
~~~

Do not make that substitution until decodedRawBody is proven to round-trip exactly for every accepted request body.

### A4. Manual OAuth protocol

File: [packages/showcase/worker/demo-auth.ts](../../packages/showcase/worker/demo-auth.ts)

Symbols: githubProvider and googleProvider

Current githubProvider excerpt:

~~~ts
authorizeUrl({ state, redirectUri }) {
  const url = new URL('https://github.com/login/oauth/authorize');
  url.searchParams.set('client_id', options.clientId);
  url.searchParams.set('redirect_uri', redirectUri);
  url.searchParams.set('state', state);
  // No scopes: public identity is all the demo needs.
  return url.toString();
},
async exchange({ code, redirectUri }) {
  const tokenResponse = await fetchFn(
    'https://github.com/login/oauth/access_token',
    {
      method: 'POST',
      headers: {
        accept: 'application/json',
        'content-type': 'application/json',
      },
~~~

Current googleProvider excerpt:

~~~ts
const tokenResponse = await fetchFn(
  'https://oauth2.googleapis.com/token',
  {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'authorization_code',
      code,
      client_id: options.clientId,
      client_secret: options.clientSecret,
      redirect_uri: redirectUri,
    }).toString(),
  },
);
~~~

The providers construct authorization URLs, send token requests, cast JSON responses, and fetch provider profiles inside repository code. The migration target is:

~~~ts
interface OAuthProvider {
  authorizationUrl(input: AuthorizationInput): Promise<URL>;
  exchange(input: CallbackInput): Promise<ProviderIdentity>;
}
~~~

The concrete implementation can wrap openid-client, but callers must continue to depend on the local seam. Session and actor construction remain outside the package adapter.

### A5. D1-shaped SQLite facsimile

File: [packages/flowsafe/test-support/sqlite.ts](../../packages/flowsafe/test-support/sqlite.ts)

Symbol: d1DatabaseLike

~~~ts
export function d1DatabaseLike(db: SqliteDatabase): unknown {
  const runSync = Symbol('runSync');
  let batchTail: Promise<void> = Promise.resolve();

  function statement(sql: string, params: unknown[]): Record<string, unknown> {
    const execute = () => {
      const outcome = db.prepare(sql).run(...params) as {
        changes?: number | bigint;
      };
      return {
        success: true,
        meta: { changes: Number(outcome?.changes ?? 0) },
      };
    };
~~~

The adapter also implements prepare, bind, first, run, all, raw, exec, batch, and dump. Its batch path serializes operations through batchTail and manually issues BEGIN IMMEDIATE, COMMIT, and ROLLBACK. That is useful as a unit fake but cannot establish Cloudflare runtime fidelity or concurrency behavior.

### A6. Custom Vite Worker emulator

File: [packages/showcase/run-api-dev-plugin.ts](../../packages/showcase/run-api-dev-plugin.ts)

Symbol: createInMemoryHub, current documented limitation

~~~ts
/**
 * A process-global in-memory hub mirroring InMemoryApprovalStore's role: a
 * structural HubNamespaceLike whose deployment stub records every published
 * ApprovalStreamEvent. pnpm dev hosts no
 * WebSocket upgrade — Vite's connect middleware cannot complete a WS handshake
 * and adding a raw ws server is out of scope — so /subscribe answers 426 and
 * the client degrades to polling (DL-019);
 */
~~~

File: [packages/showcase/vite.config.ts](../../packages/showcase/vite.config.ts)

Current plugin assembly:

~~~ts
plugins: [react(), ...(command === 'serve' ? [runApiDevPlugin()] : [])],
~~~

Target:

~~~ts
plugins: [react(), cloudflare()],
~~~

The final config may need command-specific options, but it must not retain a parallel in-memory API implementation.

### A7. Custom body reader

File: [packages/flowsafe/src/http-body.ts](../../packages/flowsafe/src/http-body.ts)

Symbol: readBoundedBytes

~~~ts
const reader = request.body?.getReader();
const chunks: Uint8Array[] = [];
let length = 0;
if (reader) {
  for (;;) {
    const next = await reader.read();
    if (next.done) break;
    length += next.value.byteLength;
    if (length > maxBytes) {
      await reader.cancel(cancelReason);
      return { ok: false, reason: 'payload-too-large', bytesRead: length };
    }
    chunks.push(next.value);
  }
}
~~~

Symbol: readBoundedBody

~~~ts
text: new TextDecoder('utf-8', {
  fatal: true,
  ignoreBOM: false,
}).decode(raw.bytes),
~~~

Hono bodyLimit can replace ordinary request limits. The byte reader must remain for signature and exact-byte boundaries unless the package proves every existing behavior.

### A8. Manual router and schema

File: [packages/flowsafe/src/agent-host/router.ts](../../packages/flowsafe/src/agent-host/router.ts)

Symbol: AgentRouter

~~~ts
export type AgentRouter = (request: Request) => Promise<Response | null>;
~~~

Symbol: match

~~~ts
function match(url: URL): MatchedRoute | undefined {
  const segments = url.pathname.split('/').filter(Boolean);
  if (segments[0] !== 'agents') return undefined;
  if (segments.length === 1) return { kind: 'catalog', allow: 'GET' };
  const agentId = decoded(segments[1]);
  if (agentId === undefined) return { kind: 'not-found' };
~~~

Symbol: readStartBody

~~~ts
const body = parsed as Record<string, unknown>;
const fields = Object.keys(body);
const forbidden = fields.find((field) => field !== 'prompt');
if (forbidden !== undefined) {
  return json({ error: "field '" + forbidden + "' is not allowed" }, 400);
}
~~~

The target strict schema must preserve the one-field object rule, whitespace rejection, 10,000-code-unit limit, 16,384-byte body limit, fatal UTF-8 behavior, and current error messages.

### A9. Custom Markdown parser

File: [scripts/docs-check.mjs](../../scripts/docs-check.mjs)

Symbols to replace with AST operations:

- decodeHtmlEntities
- headingText
- githubSlug
- collectMarkdownAnchors
- sanitizedMarkdown
- collectMarkdownLinks

Current githubSlug:

~~~js
export function githubSlug(value) {
  return headingText(value)
    .toLowerCase()
    .replace(
      /[^\p{Letter}\p{Number}\p{Mark}\p{Extended_Pictographic}\s_-]/gu,
      '',
    )
    .replace(/\s/g, '-');
}
~~~

Current collectMarkdownAnchors scanning excerpt:

~~~js
const lines = markdown.split(/\r?\n/);
let fence;
let previousLine;

for (const line of lines) {
  const fenceMatch = line.match(/^\s{0,3}(`{3,}|~{3,})/);
  if (fenceMatch) {
    const delimiter = fenceMatch[1];
    if (!fence) fence = delimiter;
    else if (fence[0] === delimiter[0] && delimiter.length >= fence.length) {
      fence = undefined;
    }
~~~

These functions combine regular expressions and manual scans for fences, comments, headings, links, entities, and fragments. Repository policy code that consumes their results remains in scope for reuse.

### A10. Custom import graph scanners

Files:

- [packages/flowsafe/src/agent-host/import-isolation.test.ts](../../packages/flowsafe/src/agent-host/import-isolation.test.ts)
- [packages/flowsafe/src/host-kit/barrel-isolation.test.ts](../../packages/flowsafe/src/host-kit/barrel-isolation.test.ts)
- packages/agent-starter/scripts/check-public-imports.mjs

The two tests parse specifiers with regular expressions and manually resolve JavaScript specifiers to TypeScript, TSX, and index candidates. The agent-starter script separately scans source text. Replace syntax and resolution logic, not the actual layering rules.

Current specifiers parser from agent-host/import-isolation.test.ts:

~~~ts
function specifiers(source: string): string[] {
  const found: string[] = [];
  for (const pattern of [
    /(?:^|\n)\s*(?:import|export)\b[\s\S]*?from\s*['"]([^'"]+)['"]/g,
    /(?:^|\n)\s*import\s*['"]([^'"]+)['"]/g,
    /\bimport\s*\(\s*['"]([^'"]+)['"]\s*\)/g,
  ]) {
    for (const match of source.matchAll(pattern)) {
      if (match[1]) found.push(match[1]);
    }
  }
  return found;
}
~~~

Current source-text boundary check from agent-starter:

~~~js
const forbidden = [
  /@proofoftech\/(?:flowsafe|breakwater)\/(?:src|dist)\//,
  /packages\/(?:flowsafe|breakwater)\/src\//,
  /\.\.\/\.\.\/(?:flowsafe|breakwater)\//,
];
~~~

### A11. Custom packed-consumer checks

Files:

- [packages/breakwater/scripts/packed-consumer-test.mjs](../../packages/breakwater/scripts/packed-consumer-test.mjs)
- [packages/flowsafe/scripts/agent-host-pack-test.mjs](../../packages/flowsafe/scripts/agent-host-pack-test.mjs)
- [packages/flowsafe/scripts/signals-client-pack-test.mjs](../../packages/flowsafe/scripts/signals-client-pack-test.mjs)
- [packages/flowsafe/scripts/provisioning-pack-test.mjs](../../packages/flowsafe/scripts/provisioning-pack-test.mjs)

The scripts build and pack packages, extract tarballs, inspect manifests and exports, inspect declaration files, create temporary consumers, and invoke TypeScript. Publint and Are the Types Wrong should replace only the standard package-format portion.

Current packed-consumer excerpt:

~~~js
run('pnpm', ['run', 'build']);
run('pnpm', ['pack', '--pack-destination', packedDirectory]);

const tarballs = (await readdir(packedDirectory)).filter((name) =>
  name.endsWith('.tgz'),
);
assert.equal(
  tarballs.length,
  1,
  'pnpm pack must produce exactly one tarball',
);
const tarball = join(packedDirectory, tarballs[0]);
run('tar', ['-xzf', tarball, '-C', extractedDirectory]);

const packedPackageRoot = join(extractedDirectory, 'package');
const manifest = JSON.parse(
  await readFile(join(packedPackageRoot, 'package.json'), 'utf8'),
);
~~~

### A12. Raw SQL is a retained domain invariant

File: [packages/breakwater/src/connector-sdk/d1-idempotency-store.ts](../../packages/breakwater/src/connector-sdk/d1-idempotency-store.ts)

Symbol: D1IdempotencyStore.reserve

The current reservation protocol uses insert-on-conflict and a state- and timestamp-guarded update. Those predicates implement ownership and lease takeover. An ORM must not hide or relax them.

Current insertion:

~~~sql
INSERT INTO ${this.#table} (key, state, result, token, created_at, updated_at)
VALUES (?, 'pending', NULL, ?, ?, ?)
ON CONFLICT(key) DO NOTHING
RETURNING key
~~~

Current stale-lease takeover:

~~~sql
UPDATE ${this.#table} SET updated_at = ?, token = ?
WHERE key = ? AND state = 'pending' AND updated_at < ?
RETURNING key
~~~

Verify the exact current statements before any future data-layer proposal.
