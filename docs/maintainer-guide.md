# Maintainer guide

This guide covers repository development and release operations. Consumer setup is in [Getting started](getting-started.md).

## Branches

- `dev` is the integration branch. Feature and fix pull requests target `dev`.
- `main` is the release and production branch.
- Refresh `origin/*` before comparing, merging, or deleting branches.
- Do not back-sync `main` into `dev`; the release path promotes the already versioned `dev` state.

## Local setup

```bash
corepack enable
pnpm install --frozen-lockfile
```

The root `package.json` `engines` field sets the Node and pnpm versions the workspace requires, and `packageManager` pins the expected pnpm version. `pnpm-workspace.yaml` applies a minimum package release age with documented exceptions. The major-scoped (`name@major`) overrides in the root `package.json` pin each legacy line forward until a maintainer bumps it by hand.

## Verification

Run these after dependency installation. `pnpm test` leaves out the direct scenario projects, which CI runs in gating jobs of their own; run them locally with `pnpm test:direct-scenario` (below).

```bash
pnpm github:check
pnpm github:check:test
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm test:node-tools
pnpm docs:check
pnpm docs:check:test
pnpm docs:api
pnpm test:release-order
pnpm test:release-invocation
pnpm test:packed-breakwater
pnpm test:packed-fleet-control
pnpm test:packed-flowsafe-agent-host
pnpm test:packed-flowsafe-provisioning
pnpm --filter @proofoftech/flowsafe test:signals-client-export
pnpm --filter @proofoftech/flowsafe typecheck:react18
pnpm --filter showcase run react-doctor
pnpm --filter @proofoftech/flowsafe spike:verify
pnpm test:conformance-config
pnpm conformance:verify
```

Additional local checks:

```bash
pnpm --filter @proofoftech/flowsafe example:gtm
pnpm --filter anchorage-agent-starter test
pnpm --filter anchorage-agent-starter typecheck
pnpm --filter showcase test
pnpm --filter showcase build
```

Do not replace the React Doctor wrapper with an unpinned `pnpm dlx` command,
move its pin to an unaudited commit, or treat an incomplete report as a
passing scan.

`spike:verify:llm` is a credentialed manual proof, not a merge requirement.

### Direct scenario projects

`pnpm test:direct-scenario` runs the direct scenario Vitest projects. `fleet-control-direct-scenario` takes about thirty-five minutes on a workstation. `fleet-control-direct-scenario-seams` contains the process-loss titles and takes about fifty-five minutes, with fifteen-to-twenty-minute quiet gaps. The projects drive the reference and tenant Workers through full runs on local workerd with native bindings. They need the package dists, so run `pnpm build` first. They reach no network. `pnpm test:direct-scenario:fast` and `pnpm test:direct-scenario:seams` run the projects separately. `pnpm test` runs the root workspace without them.

To run one title, select it through the project's own config so its timeout and includes apply:

```bash
pnpm --filter @proofoftech/fleet-control exec vitest run \
  --config vitest.direct-scenario-seams.config.ts \
  test/direct-credentialed-scenario.seams.test.ts -t '<title>'
```

Put `--reporter=verbose` directly after `run` when you want the per-title lines; the project prints nothing between titles otherwise. Send the output to a file rather than through a pager or `tail`: a killed run then keeps what it printed.

`pnpm test:packed-fleet-control` installs the packed tarball into a temporary consumer under `$TMPDIR`, so it needs a writable pnpm store and about two minutes; a sandbox that mounts the store read-only cannot run it, and the failure reads as an `EROFS` on the store, not as a test failure.

## Tooling conventions

- Root `pnpm lint` runs one Biome pass.
- Root `pnpm test` runs one Vitest workspace.
- Pre-commit runs Biome on staged files and checks the complete `.github` YAML directory through lint-staged when a `.github/**/*.{yml,yaml}` file is staged.
- Pre-push runs react-doctor against changed React files.
- Showcase source uses its configured absolute aliases rather than relative cross-directory imports.
- Generated `dist/`, Wrangler state, TypeDoc output, and test artifacts are not hand-edited.
- Every public source file carries an SPDX license header.

## Changesets

Add a changeset for every user-visible package change:

```bash
pnpm changeset
```

Choose the package and impact based on the pre-1.0 compatibility policy. Explain behavior, migration, and security consequences in user language.

The changesets base is `dev`. `onlyUpdatePeerDependentsWhenOutOfRange` prevents a compatible breakwater release from forcing an unnecessary flowsafe version change.

## Release flow

1. Merge feature and fix pull requests, including their changesets, into `dev`.
2. The version workflow maintains a `Version Packages` pull request against `dev`.
3. Review generated versions and changelogs, then merge that pull request into `dev`.
4. Run the full gate on the versioned `dev` commit.
5. Open the promotion pull request from `dev` to `main`.
6. Confirm the promotion contains no pending changeset files.
7. Merge to `main`.
8. The release workflow publishes unpublished package versions to npm with provenance, creates tags, and creates GitHub releases. It publishes the `PUBLISH_PREREQUISITES` packages in `scripts/publish-ordered.mjs` before the rest, so each exact or minimum package dependency is available first.
9. Confirm npm tarballs, export smoke tests, release notes, Pages API docs, and the production showcase.

The release workflow never opens version pull requests or commits to `main`. A pending changeset on `main` is a freeze-window failure.

## Mastra compatibility

CI tests the declared supported peer version as part of the normal gate. A separate non-blocking canary runs the library suites against the newest Mastra 1.x.

Treat a red canary as a release investigation even though it does not block a merge. Update the declared peer range only after tests, workerd proofs, package tarball probes, and migration notes pass.

The canary's typecheck and test steps cannot see a published-dist bundling regression: they do not link Mastra's shipped output through a bundler. `pnpm --filter @proofoftech/flowsafe spike:bundle` is the canary's bundling proof; against the pinned peer, `verify-core` carries that role. `spike:bundle-check` runs the Breakwater build and then `spike:bundle`, stopping at a build red; run `spike:bundle` after a build to see the bundle's own result. Note that the `--outdir .wrangler/bundle-check` in `spike:bundle` resolves relative to the wrangler CONFIG directory, not the working directory, so the output lands in `packages/flowsafe/spike/.wrangler/bundle-check`; a working-directory-relative path silently writes one level deeper, outside the ignored path. Each newest-core probe step carries its own `continue-on-error`, so one probe's red, or an expected upstream failure, still lets the steps after it run. A final step carries their outcomes to the job status, so an expected upstream red reds the `mastra-compat` job, the CI run, and the README badge; it gates no merge, because the required check `verify` does not list that job.

The durable agent surface has its own tripwire. `packages/flowsafe/src/agent-runner/durable-agent-surface.test.ts` classifies every own member of Mastra's `DurableAgent.prototype`, and fails on any member the file does not classify. On a core upgrade it therefore demands reading the new member's implementation in the installed dist before classifying it. Never satisfy it by widening the non-execution list without that read. It pins the inherited `Agent.prototype` members the same way, since Mastra calls the agent instance and the instance inherits both surfaces. Breakwater carries its own inventory of `Agent.prototype` in `packages/breakwater/src/agent/agent.test.ts`, classifying the same surface for what a narrowed guarded handle may expose.

Per-suspension deadlines couple to undocumented Mastra behavior: a step arms a deadline through a reserved key in the payload it hands `suspend()`, which only reaches flowsafe because Mastra substitutes the schema-parsed suspend payload into the run summary (verified in the declared peer). A change there — a different substitution, a different key for a nested suspension, or resume-data validation moving — silently disarms every deadline. Tripwire tests in `packages/flowsafe/src/do-runner/runtime.test.ts` pin the observed behavior. The same tests pin the in-memory fallback's `isFromInMemory` marker and the lifecycle status it reports, including `suspended` without suspended paths. Check them on every Mastra upgrade and treat a failure as a behavior change to document, never as a test to relax.

Rolling this release back is not symmetric: 0.17.x has no deadline reader, so the first alarm a downgraded run object takes deletes the alarm and orphans every armed record. Re-upgrading heals only runs that later receive another lifecycle boundary — which excludes exactly the runs a suspension deadline exists for, since a suspended run waiting on a signal has no boundary but its own wake. Prefer rolling forward; if a downgrade is unavoidable, treat every deadline armed before it as lost.

Breakwater installs its client tool outcome recorder through core's TS-private `Agent.resolveInputProcessors` on the standard loop and through `listInputProcessors` on the durable loop; see `packages/breakwater/src/agent/index.ts`. In that file, `applyInputMessages` calls core's TS-private `MessageList.pushMessageToSource` to return a remembered message with a caller client tool outcome to the input set. The `refuses a returned remembered client outcome as $shape` and `maps a returned remembered client outcome as $shape without re-reading history and saves it on standard loops` rows in `packages/breakwater/src/agent/input-chain.test.ts` guard this coupling. `callerMessages` in `packages/breakwater/src/processor-additions.ts` selects caller input from merged messages; its selection depends on core merging client tool outcomes into remembered messages by message id, tool-call id and state. The `client tool outcomes merged into memory` describe block in `packages/breakwater/src/agent/caller-input.test.ts` and FlowSafe's `durable client tool outcomes merged into memory` describe block in `packages/flowsafe/src/agent-runner/durable-agent-runner.test.ts` are standard- and durable-loop counterparts, read the same way. A bypassed recorder turns the allow rows red because `callerMessages`' no-record fallback over-evaluates stored outcomes, and turns the duplicate-outcome row red because nothing runs `recordClientToolOutcomes` to refuse the duplicate; deny rows stay green. Red deny rows signal changed core merge semantics; `caller-input.test.ts`'s `stored model outputs a caller message carries` describe block also checks `callerMessages`' no-record fallback on a plain Mastra agent. Investigate a failure as a behavior change, never as an expectation to update.

### Core bump maintenance contract

A `@mastra/core` bump carries these obligations:

- The reason table (`BLOCKED_RUN_ENTRIES` in `durable-agent-runner.ts`) is authoritative, and `durable-agent-surface.test.ts` is what forces the read.
- When the table gains or loses an entry, update the grounds list in [Durable agents](durable-agents.md) in the same commit.
- Check Breakwater's `forwardClassified` and the surface test's `VERSION_SKEW` before moving the pin. Their expiry and staleness assertions identify entries whose version claims need updating.
- `packages/flowsafe/src/do-runner/core-events-nonce.ts` stores the value `@mastra/core/events` reads under `Symbol.for('@mastra/core/unix-socket-pubsub/process-nonce')` before the entry loads: core generates it with `crypto.randomUUID()` at module scope, where workerd refuses random values. On a bump, read the module-scope statements in the new core's `dist/events/index.js`. Once core stops generating the nonce at module scope, delete the module, its import, its `sideEffects` entries, and the `HostPubSub` startup-limit docstring sentence. A renamed key or another module-scope operation workerd refuses still bundles but fails Worker startup; `spike:verify` and the packed agent-host probe show that failure, while bundling alone cannot.
- `FencedWorkflowsStorageD1` persists workflow snapshots through its own upsert (`#persistUnlessSettled` and `mastraSnapshotRow` in `packages/flowsafe/src/do-runner/fenced-workflows-d1.ts`) instead of `@mastra/cloudflare-d1`'s, so a `@mastra/cloudflare-d1` bump carries the same obligation: read the new adapter's `persistWorkflowSnapshot`. Its `satisfies Record<keyof PersistInput, unknown>` capture stops compiling on a new persist argument, and the parity row in `fenced-workflows-d1.test.ts` fails when the adapter writes a row differently.
- `packages/flowsafe/src/do-runner/d1-storage.ts`'s exhaustive storage-domain capture is re-read: its `satisfies Record<keyof MastraStorageDomains, unknown>` stops compiling when the new core adds a domain, which the canary's FlowSafe typecheck probes report for each program that compiles it. While it is red, those probes read `failure`, and a step's log, not its summary line, shows whether its program has errors of its own. Naming a new domain in that capture does not compile against the pinned core, so new domains are classified when the pin moves.
- `GUARDED_DURABLE_CALL_OPTIONS` in `packages/flowsafe/src/agent-runner/durable-agent-runner.ts` classifies every call option of a guarded durable call, and its key type stops compiling when the new core declares a key. Classify each new key, and re-check each allowed key against the reason the table records for it. Read every place the new core's durable agent and thread runtime read call options — `execOptions` in durable preparation, the stream adapter's destructured options, and the idle-loop and resume helpers; `grep -o -E 'execOptions\??\.[A-Za-z_]+'` over `create-durable-agent-*.js` finds only the first. A read of a key neither type declares that bypasses a guarded property goes into a named union added to the table's key type, then into the table. Check which keys core's own `stream()` re-entries pass, its signal drains and idle loop: a refused key there fails every signal drained into a guarded thread. Update the list in [Durable agents](durable-agents.md#durable-call-options) in the same commit.
- `vitest.flowsafe-workers.config.ts` enables, by compatibility flag, each Node module that `@mastra/core`'s import graph needs and the test Worker's compatibility date lacks. On a bump, run `pnpm exec vitest run --config vitest.flowsafe-workers.config.ts`: a newly imported module stops the test module from loading (reported as `Worker exited unexpectedly` or as `No such module`, depending on the pool version). Add the module's `enable_nodejs_*` flag.
- `durable-agent-runner.ts` writes Mastra's run-registry entries directly. `#rehydrateRegistry` rebuilds both entries of a resumed leg and deletes the global one first, because the global registry is a cache created with `noDisposeOnSet`, so replacing an entry does not dispose it; its `composeTotalBudget` mirrors core's private `#installAbortWithTotalTimeout`. `executeWorkflow` replaces a start leg's `abortSignal` on both entries with one linked to the leg's abort, which reaches the calls that read the signal before an eviction. `persistedModelSettings` reads the `modelSettings` core persists in the loop's workflow input. On a bump, re-read where core's steps read `abortSignal`, `#installAbortWithTotalTimeout`, the cache's dispose options and the persisted input's shape. The `FlowsafeDurableAgent abort of the model and tool calls in flight` block in `durable-agent-runner.test.ts` turns red when one of them moves.

## Public documentation

`pnpm docs:check` runs the checks `scripts/docs-check.mjs` composes in `checkRepository`. `pnpm docs:api` builds the API reference, compiling the React UI in its own TypeScript program.

Do not place implementation plans or agent instructions in the public navigation. Uncommitted designs belong under `docs/proposals/` with an explicit proposal banner.

## Deployment ownership

The production showcase is a single Cloudflare Worker that serves its SPA and API at `anchorage.proofoftech.org`. Production deployment happens from `main` after the workspace build.

Repository administrators separately own:

- GitHub Pages for generated API docs;
- private vulnerability reporting;
- npm trusted publishing or `NPM_TOKEN`;
- the showcase's Cloudflare bindings, secrets, domain, and OAuth callback;
- branch protection and required checks.

Do not change those external controls as a side effect of an unrelated code change.
The `protect main` ruleset requires the status check named `verify`, the gate job
in `ci.yml`. Read that job for the rule it applies to its `needs` list; a new
gating job joins that list, not the ruleset. A push to `main` starts `ci.yml` and
`release.yml` concurrently, and the release workflow does not wait for CI's
result.
