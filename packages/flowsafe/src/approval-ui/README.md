# approval-ui

## Overview

Standalone React dashboard for the approval queue. It talks only to the
approval REST API — no Mastra, no direct store access — so it can be hosted
anywhere and versioned against the wire contract alone.

## Design Decisions

- **Styling-library agnostic (slot injection).** The views render every visual
  primitive through injected slot components — the `ApprovalUIComponents`
  contract + `ApprovalUIProvider` context in `components.tsx` — with a plain-HTML
  default adapter (`htmlComponents`, `flowsafe-*` class hooks). An importer
  supplies their own design system (the showcase ships Astryx adapters in
  `packages/showcase/src/astryx-components.tsx` and
  `packages/showcase/src/astryx-stream-components.tsx`); the library has no
  design-system or CSS dependency and runs on React 18 or 19. This is the
  react-select / react-markdown / MDX `components` pattern.
  `useApprovalDashboard` is the headless core beneath the views, so a consumer
  can skip the views entirely and render a fully custom UI. Rejected: baking in
  one design system (couples every consumer to it and forces its React version —
  the concrete failure that pulling Astryx as a hard dep caused); CSS-only
  theming (can't swap in a consumer's own components).
- **`InfoTip` slot + `APPROVAL_TIPS`.** The views attach hover explanations to
  domain terms (SLA, status values, grants, metrics) through one additional
  slot, `InfoTip {label: ReactNode, tip: string}`, with a plain
  `<span title>` default — so a design-system adapter can supply a real
  Tooltip without the library depending on one. The copy lives in `tips.ts`
  (`APPROVAL_TIPS`), exported for consumers to reuse. Because tips must ride
  table headers, `ApprovalColumn.header` widened `string → ReactNode` — a
  custom Table slot doing string operations on `header` must treat it as a
  node now (the built-in HTML and app Astryx tables already do).
- **Triage is additive.** The batch/filter layer (2026-07-11) added two slots
  — `Checkbox` and `Select` — as OPTIONAL members of `ApprovalUIComponents`,
  so an adapter typed against the FULL interface before 0.2.0 keeps
  compiling, not just `Partial`-typed ones; the provider merge fills omitted
  slots from the HTML defaults, and the views consume
  `ResolvedApprovalUIComponents` (every slot present). Post-1.0 slot
  additions follow the same optional-member pattern. `FilterBar` maps its drafts to an `ApprovalListFilter`
  through the pure `buildTriageFilter` (age presets become `createdBefore` at
  APPLY time from an injected clock), and the hook's `setFilter` override is
  DERIVED against the options filter's value (`effectiveApprovalFilter`) —
  a caller-side filter change retires it on the same render, no reset
  effect. Batch selection is derived-pruned (`pruneSelection`) to open
  records still in the fetched page, so a decided or paged-out record can
  never ride a stale checkbox into `decideSelected`. Domain types
  (`BatchDecideResult`, `ApprovalRecord`, …) stay on the package ROOT
  barrel; this subpath exports only UI surface.
- **Subpath export only.** The dashboard ships as
  `@proofoftech/flowsafe/approval-ui` and is deliberately absent from the
  package root barrel, so DO-runner/API consumers never resolve React.
  `react`/`react-dom` are optional peers for the same reason.
- **Four-tsconfig scheme instead of a separate package.** JSX needs the DOM
  lib, but the rest of the package compiles against the
  `@cloudflare/workers-types` ambient set, and the two declare conflicting
  globals (`Request`/`Response`/`fetch`). So the `.tsx` shells, `mount.tsx`,
  the UI barrel, and the React hook `use-approval-dashboard.ts` compile only in
  this directory's own tsconfig (`jsx: react-jsx`, `lib: DOM`, `types: []`),
  while the package/test/demo tsconfigs exclude them (`.tsx` via its glob; the
  React `.ts` hooks via a `use-*.ts` glob, since they aren't caught by `*.tsx`).
  The fourth pass is `tsconfig.test.json` here: identical settings with the
  test exclusion lifted (`exclude: []`, `noEmit`), owning JSX-importing tests
  (`components.test.ts`) that the workers-typed package test pass must exclude.
  A separate `@proofoftech/flowsafe-ui` package
  would remove the double compilation entirely and is the documented
  fallback if the UI grows.
- **No jsdom render tests — one renderer-backed exception for hook wiring.**
  The components are thin declarative shells; everything decision-worthy
  (SLA math, queue ordering, wire formats, error mapping) is extracted into
  `view-model.ts` and `client.ts` and tested in plain node. Rejected:
  @testing-library/react + jsdom — three more age-gated dev dependencies
  plus a third test environment for marginal coverage of markup. Amended
  2026-07-11: that rationale covers markup, but the filter-identity request
  loop lived in `useApprovalDashboard`'s dependency wiring (the
  useMemo/useCallback/effect interplay), which no pure extraction can
  execute — `approvalFilterKey`'s stability test cannot prove the mounted
  hook stops refetching. `use-approval-dashboard.render.test.ts` is the one
  renderer-backed suite: raw `createRoot` + `act` on `happy-dom` (a single
  dev dependency, scoped per-file via `@vitest-environment` — no
  @testing-library, no jsdom, no global environment change), proven to fail
  against the pre-fix hook. Markup stays untested by design; the exception
  covers hook dependency wiring only.

## Invariants

- `client.ts` and `view-model.ts` must stay free of ambient DOM and
  workers-types globals (fetch is injected behind the structural
  `FetchLike`). They compile in BOTH the main pass and the UI pass; a DOM
  global introduced there breaks the main (workers-types) build far from
  the cause. The double emit is byte-identical by construction — `lib`
  affects type-checking, not emitted JS.
- Wire types are imported from `../approval-api` source (same package);
  the REST JSON contract IS those types serialized.
- `useApprovalDashboard` derives selection from the fetched list (no
  state+effect mirroring); polling is the one legitimate effect.

## Live streaming (Part B)

Opt-in live updates over an INJECTED, DOM-free `StreamTransport` (a structural
seam like `FetchLike`), so the library never hard-depends on a browser
`WebSocket`. Pass `useApprovalDashboard` a `stream: { transport, ticket }` and it
subscribes to the deployment approval stream and live-merges events on top of the
interval poll, which keeps running as the periodic reconciler. Absent
`stream`, behavior is byte-identical to poll-only.

- `stream.ts` (DOM-free, compiles in the main pass): the `StreamTransport` /
  `StreamConnection` / `StreamHandlers` seam, the `StreamFrame` wire union, and
  the PURE node-testable reducers — `mergeApprovalEvent` (upsert-by-id + re-sort),
  `applyOptimisticDecide` / `reconcileDecided` (an authoritative decided event
  clears the optimistic pending; a DIFFERENT decider surfaces a conflict),
  `presenceReducer`, `applyMetricsDelta`. A failed decide rolls the optimistic
  mark back and clears the pending so no later event raises a spurious conflict.
- `subscribeApprovalStream` (in `use-approval-dashboard.ts`): the reconnect-with-
  backoff funnel plus a client liveness HEARTBEAT — it pings and, on a missed
  pong, force-disconnects a silently half-open socket so the run poll resumes; a
  permanent (4xx) ticket failure stops retrying and stays cleanly poll-only.
- `use-web-socket-transport.ts` is the ONLY module with a browser-`WebSocket`
  dependency (read off `globalThis`, structural shape); it is a UI-pass-only file
  the main pass excludes, and it is INJECTED, never imported by the DOM-free
  modules.
- `Toast?` / `PresenceIndicator?` are additive OPTIONAL `ApprovalUIComponents`
  slots (same pattern as `Checkbox`/`Select`): defaulted in `htmlComponents`,
  consumed via `ResolvedApprovalUIComponents`, so a full-interface adapter keeps
  compiling (semver-minor).
