# breakwater/

`@proofoftech/breakwater` — Mastra safety middleware (policy engine, RBAC + audit, connector SDK).

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `README.md` | Package purpose, subpackage roles, status, usage example | Understanding breakwater's design and wiring it into a Mastra Agent |
| `CHANGELOG.md` | Release notes (0.1.0 released 2026-07-11; breakwater publishes before flowsafe) | Cutting a release, checking the publish order |
| `LICENSE` | Apache-2.0 copy shipped in the npm tarball (packing ignores the repo-root LICENSE) | Never edit separately — keep identical to the root LICENSE |
| `CONNECTORS.md` | Connector authoring guide — manifest field-by-field with enforcement semantics, honesty rules, testing expectations, contribution flow | Building or reviewing a connector, onboarding community contributors |
| `package.json` | Manifest, subpath exports (`./policy-engine`, `./rbac`, `./audit`, `./connector-sdk`), scripts, `@mastra/core` peer | Adding a subpath export, changing scripts, bumping the Mastra peer |
| `tsconfig.json` | Build TS config (emits `dist/`) | Changing build output or compiler options |
| `tsconfig.test.json` | Test-only TS config (type-checks `*.test.ts`) | Debugging test typecheck failures |
| `vitest.config.ts` | Vitest runner config | Changing test globs or runner options |

## Subdirectories

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `src/` | All source: policy-engine, rbac, audit, connector-sdk, and the barrel `index.ts` | Implementing or modifying any breakwater feature |
| `dist/` | Generated build output (`tsc` → `dist/`), gitignored | Never edit — rebuild with the Build command below |

## Build

```bash
pnpm --filter @proofoftech/breakwater build
```

## Test

```bash
pnpm --filter @proofoftech/breakwater test
```
