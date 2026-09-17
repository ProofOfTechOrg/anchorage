# Connector SDK navigation

- `index.ts`: connector construction and the enforced tool wrapper
- `contracts.ts`: the manifest, store, policy and connector declarations the siblings share
- `egress-posture.ts`: resolves the omitted `egressEnforcement` default
- `egress-conformance.ts`: case-scoped proof that a connector's traffic stays inside the guarded fetch; unrelated to `packages/agent-starter/conformance/`, which is a deployment conformance suite for the starter's Worker configuration
- `egress-conformance.fixtures.ts`: the manifests and cases the conformance suites drive
- `egress-fetch.ts`: per-hop runtime fetch enforcement
- `d1-idempotency-store.ts`: durable atomic replay protection
- `d1-rate-limit-store.ts`: shared fixed-window counters
- matching `*.test.ts` files: contract and race coverage

Read [`../../CONNECTORS.md`](../../CONNECTORS.md) and [`../../../../docs/connector-interface.md`](../../../../docs/connector-interface.md) before changing enforcement.
