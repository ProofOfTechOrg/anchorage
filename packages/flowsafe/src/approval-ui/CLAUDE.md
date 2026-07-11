# approval-ui/

React approval dashboard — a standalone app over the approval REST API,
shipped as the `@proofoftech/flowsafe/approval-ui` subpath.

**Styling-library agnostic.** The `.tsx` views render every visual primitive
through injected slot components — the `ApprovalUIComponents` contract +
`ApprovalUIProvider` context in `components.tsx`, with a plain-HTML default
adapter (`htmlComponents`, `flowsafe-*` class hooks). An importer supplies their
own design system (the app supplies an Astryx adapter in `../../app/src/`); the
library itself has **no** Astryx/StyleX dependency and no CSS to bundle, and
works on React 18+ (only `useId` and older hooks). `useApprovalDashboard`
(`use-approval-dashboard.ts`) is the headless core — all data + interaction
logic, no markup — usable on its own to drive a fully custom UI.
`client.ts`/`view-model.ts` stay DOM-free (they compile in the main
workers-typed pass too); the hook and the `.tsx` are React-only and compile
only in the UI pass.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `README.md` | Invisible knowledge: subpath-only/optional-peer decision, the three-tsconfig scheme and its ambient-free invariant, the no-jsdom stance + its one renderer-backed exception (hook-wiring regression on happy-dom) | Before adding UI files, touching any flowsafe tsconfig, or adding DOM usage to shared modules |
| `client.ts` | `ApprovalApiClient` (injected-fetch REST client, structural `FetchLike`), `ApprovalApiError` | Changing API calls or error mapping |
| `view-model.ts` | Pure presentation logic: SLA state/countdowns, queue ordering (`sortQueue` delegates to approval-api's shared `byReviewerOrder` so the client sort and the stores' `orderBy: 'reviewer'` bounded pages can never rank differently), duration formatting | Changing queue order or SLA display rules |
| `use-approval-dashboard.ts` | Headless `useApprovalDashboard` hook: fetch/poll, derived selection, busy/error state, claim/decide/delegate actions, and the triage layer — `setFilter` (a derived override guarded by `effectiveApprovalFilter`: it holds only while options.filter keeps the VALUE it was set against, so a caller-side change retires it with NO reset effect), batch selection (`selectedIds` derived-pruned by `pruneSelection` to open records still in the page), `decideSelected` → `client.decideBatch`, `lastBatch` (cleared when the next mutation starts). `DEFAULT_QUEUE_FILTER` is open statuses + `limit: 100` + `orderBy: 'reviewer'` (the server ranks before cutting the page). refresh() is keyed on the EFFECTIVE filter's VALUE (`approvalFilterKey` + a latest-ref for `now`), so inline `filter`/`now` options poll on the interval instead of looping requests; `client` identity stays a deliberate refetch signal. UI-pass-only (React), excluded from the main pass | Changing dashboard data, filter override, selection, or batch logic |
| `components.tsx` | The `ApprovalUIComponents` slot contract (incl. `InfoTip` — hover-tip slot with a `<span title>` default; `EmptyState` takes an optional `description`; `ApprovalColumn.header` is `ReactNode` so headers can carry tips; the triage slots `Checkbox` + `Select` as OPTIONAL members — full-interface pre-0.2.0 adapters keep compiling, the merge fills them from the defaults, and views consume `ResolvedApprovalUIComponents` where every slot is present), `ApprovalUIProvider`/`useApprovalUIComponents`, and the `htmlComponents` default adapter (labeled via useId, like TextField) | Changing the styling contract or the HTML default |
| `FilterBar.tsx` | Slot-rendered queue triage controls: status/workflow/run/claimedBy/requestedBy + age presets (`AGE_PRESETS` — "older than" cutoffs mapped to `createdBefore` at APPLY time via an injected clock; no Date.now in a render path), apply-on-click (each filter change is a fetch), Reset restores `DEFAULT_QUEUE_FILTER`. The drafts→filter mapping is the pure `buildTriageFilter` (keeps the base limit/orderBy, always drops `after`) | Changing triage filter UX or the draft mapping |
| `filter-bar.test.ts` | DOM-free `buildTriageFilter`/`statusDraftOf` coverage: age math with injected nowMs, trimming, status mapping, cursor dropping (UI test pass — imports the .tsx) | Changing FilterBar logic |
| `tips.ts` | `APPROVAL_TIPS` — the hover-tip copy for domain terms (SLA, statuses, grants, metrics), DOM-free, compiled in both passes | Changing tip copy or adding a tipped term |
| `App.tsx` | Dashboard shell: runs the hook, renders the child views through the injected slots — FilterBar above the queue, the batch bar (count + shared comment + approve/reject/clear) when a selection exists, and the `lastBatch` failure Banner (`batchFailureSummary`; successes read from the refreshed queue itself) | Changing dashboard composition |
| `QueueView.tsx` | Queue table (via the `Table`/`Badge`/`Button` slots) with SLA column and row selection; optional batch mode (BOTH `selectedIds` + `onToggleSelect` provided) prepends a selection column — a `Checkbox` per OPEN row, an empty cell for decided ones | Changing the queue view |
| `DetailView.tsx` | Record detail + decision form (approve/reject/comment), claim, delegate — all via slots | Changing the decision UX |
| `MetricsView.tsx` | Metrics summary (via `MetadataList` slots) | Changing the metrics view |
| `mount.tsx` | `createApprovalDashboard(container, { client, components })` — wraps `App` in `ApprovalUIProvider` | Embedding the dashboard |
| `index.ts` | UI barrel (compiles only in the UI tsc pass) | Finding the UI export surface |
| `tsconfig.json` | The UI compilation pass: `jsx: react-jsx`, DOM lib, `types: []` | Debugging UI typecheck/build |
| `tsconfig.test.json` | The UI **test** pass: same settings, `exclude: []` + `noEmit` — owns JSX-importing tests the workers-typed package test pass must exclude | Adding a test that imports a `.tsx` module |
| `client.test.ts` | Wire-format and error-mapping tests (plain node) | Adding client tests |
| `client-router.pipeline.test.ts` | Full-pipeline D3 proof (plain node): `ApprovalApiClient` → router → service → store — a bare reviewer-ordered `list()` past the cap stays bounded yet still surfaces the freshest critical at the top | Adding cross-layer approval-flow tests |
| `use-approval-dashboard.test.ts` | DOM-free hook tests: `DEFAULT_QUEUE_FILTER` contract, poll wiring (`fetchDashboardSnapshot`), `orderRecordsForDisplay` (reviewer-only re-sort), `pruneSelection`, and the `effectiveApprovalFilter` override derivation — no renderer | Adding hook data/filter tests |
| `use-approval-dashboard.render.test.ts` | The ONE renderer-backed suite (raw `createRoot` + `act` on happy-dom via a per-file `@vitest-environment` docblock): mounts the hook and pins the P1 filter-identity fix — inline value-equal options refetch once, value/client changes refetch immediately; fails against the pre-fix hook — plus the triage WIRING (setFilter override refetch + options-change retirement, selection pruning through decideSelected → decideBatch, empty-selection no-op). Same charter: hook wiring only, markup untested. Excluded from the workers-typed package test pass (react-dom/client needs DOM types); the UI test tsconfig owns it | Changing the hook's dependency wiring, or weighing another render test against the no-jsdom stance |
| `view-model.test.ts` | SLA states, sorting, formatting tests (plain node) | Adding view-model tests |
| `tips.test.ts` | `APPROVAL_TIPS` completeness (every status/metric/concept key non-empty) | Changing tips |
| `components.test.ts` | Default `InfoTip`/`EmptyState` shapes (description rendered/omitted) + provider merge semantics (element inspection, no jsdom) | Changing the default adapter or merge behavior |
