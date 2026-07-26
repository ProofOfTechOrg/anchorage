// SPDX-License-Identifier: Apache-2.0
// @proofoftech/flowsafe — Approval UX + Cloudflare-native durable execution
//
// flowsafe plugs into Mastra's suspend/resume workflow events and adds:
// 1. Approval API — queue, claim, decide, delegate, SLA tracking, escalation,
//    and store-derived breakwater grant minting
// 2. Approval dashboard — minimal React UI for the approval queue
//    (subpath export '@proofoftech/flowsafe/approval-ui' only — a standalone
//    app over the REST API)
// 3. DO runner — init()-based import-swap for Mastra on Cloudflare Durable Objects
// 4. Audit export — Queues producer sink + batch consumer shipping audit
//    events to a SIEM over HTTP
// 5. Artifacts — R2-backed workflow artifact storage keyed by run identity
// 6. Host kit — the shared run routes, bearer auth seam, and suspension→approval
//    bridge every host mounts (subpath export '@proofoftech/flowsafe/host-kit'
//    only — it is host glue, not part of the library's core surface)
//
// Export rule: this root barrel mirrors the approval-api, do-runner,
// artifacts, and audit-export subpath barrels COMPLETELY — anything those
// subpaths export is also importable from the package root. approval-ui and
// host-kit are subpath-only by design (React peer / host glue).

export * from './approval-api/index.js';
export * from './artifacts/index.js';
export * from './audit-export/index.js';
export * from './do-runner/index.js';
