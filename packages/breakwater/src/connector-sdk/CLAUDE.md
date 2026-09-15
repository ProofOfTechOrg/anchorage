# Connector SDK navigation

- `index.ts`: permission manifest and enforced tool wrapper
- `egress-conformance.ts`: case-scoped proof that a connector's traffic stays inside the guarded fetch; unrelated to `packages/agent-starter/conformance/`, which is a deployment conformance suite for the starter's Worker configuration
- `egress-fetch.ts`: per-hop runtime fetch enforcement
- `d1-idempotency-store.ts`: durable atomic replay protection
- `d1-rate-limit-store.ts`: shared fixed-window counters
- matching `*.test.ts` files: contract and race coverage

Read [`../../CONNECTORS.md`](../../CONNECTORS.md) and [`../../../../docs/connector-interface.md`](../../../../docs/connector-interface.md) before changing enforcement.
