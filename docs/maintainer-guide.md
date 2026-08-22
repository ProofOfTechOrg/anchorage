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

The workspace requires Node 22.22.0 or later and pnpm 10.16 or later. `packageManager` pins the expected pnpm version. `pnpm-workspace.yaml` applies a seven-day minimum package release age with documented exceptions for lockstep or tool-imposed dependencies. Versioned overrides for `js-yaml@3`, `js-yaml@4`, and `nanoid@3` pin each legacy line forward until a maintainer bumps it by hand.

## Verification

The commands below mirror the CI `verify` job after dependency installation, in order:

```bash
pnpm github:check
pnpm github:check:test
pnpm lint
pnpm typecheck
pnpm test
pnpm build
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

The React Doctor wrapper pins an audited commit, blocks on warnings, and
rejects reports with skipped or incomplete checks. Do not replace it with an
unpinned `pnpm dlx` command or treat an incomplete report as a passing scan.

`spike:verify:llm` is a credentialed manual proof, not a merge requirement.

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
8. The release workflow publishes unpublished package versions to npm with provenance, creates tags, and creates GitHub releases. It publishes breakwater before Flowsafe, then Fleet Control, so each exact or minimum package dependency is available first.
9. Confirm npm tarballs, export smoke tests, release notes, Pages API docs, and the production showcase.

The release workflow never opens version pull requests or commits to `main`. A pending changeset on `main` is a freeze-window failure.

## Mastra compatibility

CI tests the declared supported peer version as part of the normal gate. A separate non-blocking canary runs the library suites against the newest Mastra 1.x.

Treat a red canary as a release investigation even though it does not block a merge. Update the declared peer range only after tests, workerd proofs, package tarball probes, and migration notes pass.

The canary's typecheck and test steps cannot see a published-dist bundling regression: neither links Mastra's shipped output through a bundler. `pnpm --filter @proofoftech/flowsafe spike:bundle-check` is the canary's bundling proof, and against the pinned peer that role belongs to `spike:verify` and the showcase build inside `verify`. Note that its `--outdir .wrangler/bundle-check` resolves relative to the wrangler CONFIG directory, not the working directory, so the output lands in `packages/flowsafe/spike/.wrangler/bundle-check`; a working-directory-relative path silently writes one level deeper, outside the ignored path. The bundle step carries its own `continue-on-error` so an expected upstream failure still lets the tripwire suites after it run.

A second tripwire guards the durable agent surface. `packages/flowsafe/src/agent-runner/durable-agent-surface.test.ts` classifies every own member of Mastra's `DurableAgent.prototype`, and fails on any member the file does not classify. On a core upgrade it therefore demands reading the new member's implementation in the installed dist before classifying it — as a guarded entry point, a delegator, a refusal, or something that cannot drive a run. Never satisfy it by widening the non-execution list without that read. It pins the inherited `Agent.prototype` members the same way, since Mastra calls the agent instance and the instance inherits both surfaces. Breakwater carries its own inventory of `Agent.prototype` in `packages/breakwater/src/agent/agent.test.ts`, classifying the same surface for what a narrowed guarded handle may expose. The maintenance contract on a core bump: the reason table in `durable-agent-runner.ts` is authoritative, the surface test is what forces the read, the runner's module comment and [Durable agents](durable-agents.md) are updated from the table in the same commit — never left to drift behind it — and Breakwater's `forwardClassified` allowlist is pruned of every name the new pin now exposes, which its own test asserts.

Per-suspension deadlines couple to one undocumented Mastra behavior: a step arms a deadline through a reserved key in the payload it hands `suspend()`, which only reaches flowsafe because Mastra substitutes the schema-parsed suspend payload into the run summary (verified in the declared peer, 1.53.0). A change there — a different substitution, a different key for a nested suspension, or resume-data validation moving — silently disarms every deadline. Tripwire tests in `packages/flowsafe/src/do-runner/runtime.test.ts` pin the observed behavior: the reserved key surviving a schema that declares it, being stripped by a strict schema that does not, surviving a loose schema, and a nested suspension being refused rather than armed. Check them on every Mastra upgrade and treat a failure as a behavior change to document, never as a test to relax.

Rolling this release back is not symmetric: 0.17.x has no deadline reader, so the first alarm a downgraded run object takes deletes the alarm and orphans every armed record. Re-upgrading heals only runs that later receive another lifecycle boundary — which excludes exactly the runs a suspension deadline exists for, since a suspended run waiting on a signal has no boundary but its own wake. Prefer rolling forward; if a downgrade is unavoidable, treat every deadline armed before it as lost.

## Public documentation

`pnpm docs:check` validates local links and anchors, package export coverage, TypeDoc entry coverage, npm-safe package links, orphaned public pages, stale internal markers, manifest-backed Node engine and peer-dependency claims, and one `@mastra/core` value across the `packages/*` manifests' `peerDependencies`, `devDependencies`, and `dependencies`, including private packages, with peer/devDependency parity for libraries. `pnpm docs:api` builds all supported API surfaces, including the React UI in its own TypeScript program.

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
