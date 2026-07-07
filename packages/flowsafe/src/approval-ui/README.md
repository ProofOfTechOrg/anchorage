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
  supplies their own design system (the `app/` ships an Astryx adapter); the
  library has no design-system or CSS dependency and runs on React 18+. This is
  the react-select / react-markdown / MDX `components` pattern.
  `useApprovalDashboard` is the headless core beneath the views, so a consumer
  can skip the views entirely and render a fully custom UI. Rejected: baking in
  one design system (couples every consumer to it and forces its React version —
  the concrete failure that pulling Astryx as a hard dep caused); CSS-only
  theming (can't swap in a consumer's own components).
- **Subpath export only.** The dashboard ships as
  `@proofoftech/flowsafe/approval-ui` and is deliberately absent from the
  package root barrel, so DO-runner/API consumers never resolve React.
  `react`/`react-dom` are optional peers for the same reason.
- **Three-tsconfig scheme instead of a separate package.** JSX needs the DOM
  lib, but the rest of the package compiles against the
  `@cloudflare/workers-types` ambient set, and the two declare conflicting
  globals (`Request`/`Response`/`fetch`). So the `.tsx` shells, `mount.tsx`,
  the UI barrel, and the React hook `use-approval-dashboard.ts` compile only in
  this directory's own tsconfig (`jsx: react-jsx`, `lib: DOM`, `types: []`),
  while the package/test/demo tsconfigs exclude them (`.tsx` via its glob; the
  React `.ts` hooks via a `use-*.ts` glob, since they aren't caught by `*.tsx`). A separate `@proofoftech/flowsafe-ui` package
  would remove the double compilation entirely and is the documented
  fallback if the UI grows.
- **No jsdom render tests.** The components are thin declarative shells;
  everything decision-worthy (SLA math, queue ordering, wire formats, error
  mapping) is extracted into `view-model.ts` and `client.ts` and tested in
  plain node. Rejected: @testing-library/react + jsdom — three more
  age-gated dev dependencies plus a third test environment for marginal
  coverage of markup.

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
