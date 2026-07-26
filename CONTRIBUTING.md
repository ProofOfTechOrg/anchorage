# Contributing

Anchorage is implemented and tested.
Contributions — new connectors, policies, bug fixes, and docs — are welcome.

## Where to start

1. Read [`README.md`](README.md) for the project overview, architecture, and
   package map.
2. Read [`docs/breakwater-architecture.md`](docs/breakwater-architecture.md)
   and [`docs/flowsafe-architecture.md`](docs/flowsafe-architecture.md)
   for the two package architectures.
3. To build a connector, read
   [`packages/breakwater/CONNECTORS.md`](packages/breakwater/CONNECTORS.md).

## How to contribute

- **Connectors** — wrap a tool or CLI with an enforced permission manifest;
  follow [`packages/breakwater/CONNECTORS.md`](packages/breakwater/CONNECTORS.md)
  (manifest honesty rules + the enforcement-path tests to ship).
- **Policies** — add a tool-boundary evaluator or policy-engine gate.
- **Security** — review the threat model
  ([`docs/security-threat-model.md`](docs/security-threat-model.md))
  and report privately per [`SECURITY.md`](SECURITY.md).
- **Docs & examples** — workflows that exercise the policy engine, gaps in the
  blueprint.

Every PR must pass the verification gate below; CI
(`.github/workflows/ci.yml`) runs it on push and PR.

## Development setup

```bash
git clone https://github.com/ProofOfTechOrg/anchorage.git
cd anchorage
pnpm install
# The full verification gate (what CI runs):
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm docs:check
pnpm docs:check:test
pnpm docs:api
pnpm test:packed-breakwater
pnpm --filter @proofoftech/flowsafe test:signals-client-export
pnpm --filter @proofoftech/flowsafe typecheck:react18
pnpm --filter showcase run react-doctor
pnpm --filter @proofoftech/flowsafe spike:verify
```

Lint is one Biome pass at the root, and `pnpm test` is one root vitest run
covering every package. Git hooks (husky) back the gate up:
pre-commit runs Biome on staged files (lint-staged); pre-push runs
react-doctor on the branch's changed files (`pnpm react-doctor:diff`; bypass
with `git push --no-verify`). CI also runs a non-blocking compatibility probe
against the newest `@mastra/core` 1.x release.

The showcase app uses mandatory absolute imports — `@/*` for `src`,
`#worker/*` for worker modules, `@flowsafe/*` for deep flowsafe source
imports — enforced by Biome.

`pnpm docs:check` validates local links, Markdown anchors, documentation
reachability, package README export coverage, published-package links, and
TypeDoc entry-point coverage. `pnpm docs:api` builds the generated API site in
`docs/api/`; that directory is ignored and must not be committed. The scheduled
external-link workflow runs `pnpm docs:check:external`.

Add or update tests for behavioral changes. Update the relevant package README
and authored guides when public behavior, setup, configuration, or exports
change. Add a changeset for a user-visible change to a published package; pure
repository maintenance and docs-only changes do not need one.

## Releasing

Versioning and publishing run through [changesets](.changeset/README.md), with
version bumps happening ON `dev` (bump-on-dev). Feature and fix PRs target the
`dev` integration branch and include a changeset (`pnpm exec changeset` — pick
the packages, a semver bump, and write the CHANGELOG entry) when they change
published behavior of `@proofoftech/breakwater` or `@proofoftech/flowsafe`. As
changesets accumulate, the version workflow (`.github/workflows/version.yml`)
maintains a standing **"Version Packages" PR against `dev`** with the pending
bumps + CHANGELOG entries. Releasing is two merges: merge that PR into `dev`,
then immediately cut and merge the **release PR** (`dev` → `main`). On the
push to `main`, `.github/workflows/release.yml` publishes any not-yet-published
versions to npm with provenance and pushes the release tags — it refuses,
loudly, if an unversioned changeset reached `main` (a feature merged into
`dev` between the two merges; merge the regenerated Version Packages PR and
re-cut the release PR). There is no post-release sync step: `main` never gets
commits of its own. Publishing needs the `NPM_TOKEN` repository secret (an npm
automation token with publish rights on the `@proofoftech` scope). `showcase`
is private and never publishes.

## Governance

- Maintainer: [ProofOfTechOrg](https://github.com/ProofOfTechOrg) — final
  review and merge authority.
- Contributions flow through the standard GitHub PR process.
- Anchorage does not require a Contributor License Agreement or Developer
  Certificate of Origin sign-off.

## License

By submitting a contribution, you agree that it is licensed under Apache-2.0,
the project's license. See [`LICENSE`](LICENSE).
Everything in this repository — including enterprise-oriented features such as
SSO seams and audit export — is Apache-2.0; nothing is held back for a paid
tier.

Anchorage is an independent implementation built ON Mastra. Contributions must
not fork or modify Mastra source code, wrap Mastra Enterprise features to
bypass their licensing, or copy any third-party proprietary implementation.
