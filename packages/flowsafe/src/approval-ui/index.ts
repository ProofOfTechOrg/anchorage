// SPDX-License-Identifier: Apache-2.0
// Approval dashboard — a React UI over the approval REST API (queue, detail +
// decision form, metrics). Subpath export only
// ('@proofoftech/flowsafe/approval-ui'); deliberately absent from the package
// root barrel so DO-runner/API consumers never pull React.
//
// Styling-library agnostic: the views render through injected slot components
// (see ./components) with a plain-HTML default, and useApprovalDashboard is a
// headless core you can drive a fully custom UI from. This barrel compiles in
// the UI tsc pass (jsx + DOM lib); client.ts and view-model.ts are DOM-free and
// typecheck in the main pass too, where their tests run.

export type { ApprovalDashboardProps } from './App.js';
export { App, batchFailureSummary } from './App.js';
export type {
  ApprovalApiClientOptions,
  FetchLike,
  ResponseLike,
} from './client.js';
export {
  ApprovalApiClient,
  ApprovalApiError,
} from './client.js';
export type {
  ApprovalColumn,
  ApprovalUIComponents,
  ApprovalUIProviderProps,
  BadgeProps,
  BannerProps,
  ButtonProps,
  ButtonVariant,
  CheckboxProps,
  CodeProps,
  EmptyStateProps,
  HeadingProps,
  InfoTipProps,
  MetadataItemProps,
  MetadataListProps,
  ResolvedApprovalUIComponents,
  SectionProps,
  SelectOption,
  SelectProps,
  SpinnerProps,
  StackDirection,
  StackGap,
  StackProps,
  TableProps,
  TextFieldProps,
  TextProps,
  Tone,
} from './components.js';
export {
  ApprovalUIProvider,
  htmlComponents,
  useApprovalUIComponents,
} from './components.js';
export type { DetailViewProps } from './DetailView.js';
export { DetailView } from './DetailView.js';
export type {
  AgeDraft,
  FilterBarProps,
  StatusDraft,
  TriageFilterDrafts,
} from './FilterBar.js';
export {
  AGE_PRESETS,
  buildTriageFilter,
  DEFAULT_TRIAGE_DRAFTS,
  FilterBar,
  statusDraftOf,
} from './FilterBar.js';
export type { MetricsViewProps } from './MetricsView.js';
export { MetricsView } from './MetricsView.js';
export type { CreateApprovalDashboardOptions } from './mount.js';
export { createApprovalDashboard } from './mount.js';
export type { QueueViewProps } from './QueueView.js';
export { QueueView } from './QueueView.js';
export type { MetricLabel } from './tips.js';
export { APPROVAL_TIPS } from './tips.js';
export type {
  ApprovalDashboardState,
  UseApprovalDashboardOptions,
} from './use-approval-dashboard.js';
// effectiveApprovalFilter / pruneSelection stay module-level (DOM-free tests
// import them directly): they are the hook's internal derivations, not
// consumer API — keeping them off the barrel keeps them out of the frozen
// 0.2.0 surface.
export {
  DEFAULT_QUEUE_FILTER,
  useApprovalDashboard,
} from './use-approval-dashboard.js';
export type { SlaState } from './view-model.js';
export {
  DEFAULT_SLA_WARNING_MS,
  formatDuration,
  formatResolution,
  formatSlaCountdown,
  msRemaining,
  slaStateOf,
  sortQueue,
} from './view-model.js';
