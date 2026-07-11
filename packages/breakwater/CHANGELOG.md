# @proofoftech/breakwater

## 0.1.0 — 2026-07-11

First publishable cut. Mastra safety middleware: policy engine (output channels,
deny patterns, opt-in hold-back buffering), RBAC processor, audit sink, connector
SDK (permission manifests, grant-only write approval, network-egress declaration
gate, idempotent replay with in-memory/atomic/D1 stores, fixed-window rate
limiting, dry-run, tenant isolation scoping), and approval-gated Claude Code /
Codex CLI connectors.

Publish order: this package publishes BEFORE `@proofoftech/flowsafe` (flowsafe's
`./host-kit/module` subpath types reference it as an optional peer).

Requires `@mastra/core` ^1.50.0 (peer), Node >= 22, ESM only
(`moduleResolution` `node16`/`nodenext`/`bundler`).
