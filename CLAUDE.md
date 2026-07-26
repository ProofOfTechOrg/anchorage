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
