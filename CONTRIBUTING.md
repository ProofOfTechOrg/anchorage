# Contributing

Anchorage is implemented and tested.
Contributions — new connectors, policies, bug fixes, and docs — are welcome.

## Where To Start

1. Read [`README.md`](README.md) for the project overview, architecture, and
   package map.
2. Read [`docs/breakwater-architecture.md`](docs/breakwater-architecture.md)
   and [`docs/flowsafe-architecture.md`](docs/flowsafe-architecture.md)
   for the two package architectures.
3. To build a connector, read
   [`packages/breakwater/CONNECTORS.md`](packages/breakwater/CONNECTORS.md).

## How To Contribute

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

## Development Setup

```bash
git clone https://github.com/ProofOfTechOrg/anchorage.git
cd anchorage
pnpm install
# The full verification gate (what CI runs):
pnpm lint && pnpm typecheck && pnpm test && pnpm build
# Plus the end-to-end workerd durability proof:
pnpm --filter @proofoftech/flowsafe spike:verify
```

Lint is one Biome pass at the root, and `pnpm test` is one root vitest run
covering every package (978 tests). Git hooks (husky) back the gate up:
pre-commit runs Biome on staged files (lint-staged); pre-push runs
react-doctor on the branch's changed files (`pnpm react-doctor:diff`; bypass
with `git push --no-verify`). CI additionally runs the full react-doctor gate
(100/100, `--blocking warning`) over `packages/showcase`, plus `spike:verify`.

The showcase app uses mandatory absolute imports — `@/*` for `src`,
`#worker/*` for worker modules, `@flowsafe/*` for deep flowsafe source
imports — enforced by Biome.

## Releasing

Versioning and publishing run through [changesets](.changeset/README.md).
Feature and fix PRs target the `dev` integration branch and include a changeset
(`pnpm exec changeset` — pick the packages, a semver bump, and write the
CHANGELOG entry) when they change published behavior of `@proofoftech/breakwater`
or `@proofoftech/flowsafe`. Changesets accumulate on `dev`; a **release PR**
(`dev` → `main`) brings them to `main`. On merge, the release workflow
(`.github/workflows/release.yml`) opens a "Version Packages" PR (version bump +
CHANGELOG entries); merging that PR publishes to npm with provenance and pushes
the release tags. After that publish, sync `main` → `dev` so `dev` carries the
version bump + CHANGELOG and drops the consumed changesets; the next release
then starts from a clean `dev`. Publishing needs the `NPM_TOKEN` repository
secret (an npm automation token with publish rights on the `@proofoftech`
scope). `showcase` is private and never publishes.

## Code Of Conduct

This project follows industry-standard open-source conduct guidelines. Be
respectful, constructive, and inclusive in all interactions.

## Governance

- Maintainer: [ProofOfTechOrg](https://github.com/ProofOfTechOrg) — final
  review and merge authority.
- Contributions flow through the standard GitHub PR process.
- **Significant contributions** (new subsystems, connectors, or public API
  surface) require a Contributor License Agreement, requested and approved by
  the maintainer on the PR. Small fixes and docs changes need none.

## License

Contributions will be licensed under Apache-2.0. See [`LICENSE`](LICENSE).
Everything in this repository — including enterprise-oriented features such as
SSO seams and audit export — is Apache-2.0; nothing is held back for a paid
tier.

Anchorage is an independent implementation built ON Mastra. Contributions must
not fork or modify Mastra source code, wrap Mastra Enterprise features to
bypass their licensing, or copy any third-party proprietary implementation.
