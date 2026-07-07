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
pnpm -r lint && pnpm -r typecheck && pnpm -r test && pnpm -r build
# Plus the end-to-end workerd durability proof:
pnpm --filter @proofoftech/flowsafe spike:verify
```

## Code Of Conduct

This project follows industry-standard open-source conduct guidelines. Be
respectful, constructive, and inclusive in all interactions.

## License

Contributions will be licensed under Apache-2.0. See [`LICENSE`](LICENSE).
