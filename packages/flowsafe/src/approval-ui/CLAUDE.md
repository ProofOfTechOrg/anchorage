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
| `README.md` | Invisible knowledge: subpath-only/optional-peer decision, the three-tsconfig scheme and its ambient-free invariant, why no jsdom tests | Before adding UI files, touching any flowsafe tsconfig, or adding DOM usage to shared modules |
| `client.ts` | `ApprovalApiClient` (injected-fetch REST client, structural `FetchLike`), `ApprovalApiError` | Changing API calls or error mapping |
| `view-model.ts` | Pure presentation logic: SLA state/countdowns, queue ordering, duration formatting | Changing queue order or SLA display rules |
| `use-approval-dashboard.ts` | Headless `useApprovalDashboard` hook: fetch/poll, derived selection, busy/error state, claim/decide/delegate actions. UI-pass-only (React), excluded from the main pass | Changing dashboard data or interaction logic |
| `components.tsx` | The `ApprovalUIComponents` slot contract, `ApprovalUIProvider`/`useApprovalUIComponents`, and the `htmlComponents` default adapter | Changing the styling contract or the HTML default |
| `App.tsx` | Dashboard shell: runs the hook, renders the child views through the injected slots | Changing dashboard composition |
| `QueueView.tsx` | Queue table (via the `Table`/`Badge`/`Button` slots) with SLA column and row selection | Changing the queue view |
| `DetailView.tsx` | Record detail + decision form (approve/reject/comment), claim, delegate — all via slots | Changing the decision UX |
| `MetricsView.tsx` | Metrics summary (via `MetadataList` slots) | Changing the metrics view |
| `mount.tsx` | `createApprovalDashboard(container, { client, components })` — wraps `App` in `ApprovalUIProvider` | Embedding the dashboard |
| `index.ts` | UI barrel (compiles only in the UI tsc pass) | Finding the UI export surface |
| `tsconfig.json` | The UI compilation pass: `jsx: react-jsx`, DOM lib, `types: []` | Debugging UI typecheck/build |
| `client.test.ts` | Wire-format and error-mapping tests (plain node) | Adding client tests |
| `view-model.test.ts` | SLA states, sorting, formatting tests (plain node) | Adding view-model tests |
