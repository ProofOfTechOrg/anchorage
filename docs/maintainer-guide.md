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

The workspace requires Node 22.22.0 or later and pnpm 10.16 or later. `packageManager` pins the expected pnpm version. `pnpm-workspace.yaml` applies a seven-day minimum package release age with documented exceptions for lockstep or tool-imposed dependencies.

## Verification

Run the full merge gate:

```bash
pnpm docs:check
pnpm docs:api
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm test:packed-breakwater
pnpm test:packed-flowsafe-agent-host
pnpm test:packed-flowsafe-provisioning
pnpm --filter @proofoftech/flowsafe test:signals-client-export
pnpm --filter @proofoftech/flowsafe typecheck:react18
pnpm --filter @proofoftech/flowsafe example:gtm
pnpm --filter @proofoftech/flowsafe spike:verify
pnpm --filter anchorage-agent-starter test
pnpm --filter anchorage-agent-starter typecheck
pnpm --filter showcase test
pnpm --filter showcase build
pnpm --filter showcase react-doctor
```

The React Doctor wrapper pins an audited commit, blocks on warnings, and
rejects reports with skipped or incomplete checks. Do not replace it with an
unpinned `pnpm dlx` command or treat an incomplete report as a passing scan.

`spike:verify:llm` is a credentialed manual proof, not a merge requirement.

## Tooling conventions

- Root `pnpm lint` runs one Biome pass.
- Root `pnpm test` runs one Vitest workspace.
- Pre-commit runs lint-staged on staged files only.
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
8. The release workflow publishes unpublished package versions to npm with provenance, creates tags, and creates GitHub releases. When both packages change, publish breakwater before flowsafe so the guarded-agent peer minimum is available.
9. Confirm npm tarballs, export smoke tests, release notes, Pages API docs, and the production showcase.

The release workflow never opens version pull requests or commits to `main`. A pending changeset on `main` is a freeze-window failure.

## Mastra compatibility

CI tests the declared supported peer version as part of the normal gate. A separate non-blocking canary runs the library suites against the newest Mastra 1.x.

Treat a red canary as a release investigation even though it does not block a merge. Update the declared peer range only after tests, workerd proofs, package tarball probes, and migration notes pass.

## Public documentation

`pnpm docs:check` validates local links and anchors, package export coverage, TypeDoc entry coverage, npm-safe package links, orphaned public pages, stale internal markers, and manifest-backed Node engine and peer-dependency claims. `pnpm docs:api` builds all supported API surfaces, including the React UI in its own TypeScript program.

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
