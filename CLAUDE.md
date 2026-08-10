# Repository navigation

Public product and architecture documentation lives in [`docs/README.md`](docs/README.md). Do not duplicate shipped behavior in this file.

## Read before changing code

- [`README.md`](README.md): product overview and package chooser
- [`docs/getting-started.md`](docs/getting-started.md): consumer setup
- [`docs/security-threat-model.md`](docs/security-threat-model.md): trust boundaries
- [`docs/maintainer-guide.md`](docs/maintainer-guide.md): branches, verification, and releases
- [`CONTRIBUTING.md`](CONTRIBUTING.md): contribution workflow

Package-specific navigation:

- [`packages/breakwater/CLAUDE.md`](packages/breakwater/CLAUDE.md)
- [`packages/flowsafe/CLAUDE.md`](packages/flowsafe/CLAUDE.md)
- [`packages/showcase/CLAUDE.md`](packages/showcase/CLAUDE.md)

## Package-first design check

Before implementing a new feature or substantial utility:

1. Search current package registries and official documentation for mature, maintained packages that already provide the required behavior.
2. Compare viable packages with custom code on runtime compatibility, API stability, maintenance and security history, license, release age, dependency and bundle cost, and fit with existing abstractions.
3. Present the viable packages, recommended integration boundary, benefits, risks, and custom-code alternative to the user.
4. Wait for the user's direction before installing a package or handrolling the functionality.

Reuse an existing dependency or repository utility when it already fits. A small invariant-specific adapter can remain custom, but report the package search and explain why custom code is the narrower choice. Record rejected packages and their rationale in non-trivial implementation plans.

## Commands

```bash
pnpm docs:check
pnpm docs:api
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm --filter @proofoftech/flowsafe spike:verify
```

Feature and fix pull requests target `dev`; `main` is the release branch. Refresh remote refs before reasoning about branch state.
