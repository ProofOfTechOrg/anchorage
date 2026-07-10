// Hover-tip copy for the dashboard's domain terms, rendered through the
// InfoTip slot. One source so QueueView/DetailView/MetricsView never drift
// and consumers can reuse the same explanations in their own surfaces.
// DOM-free on purpose: compiles in the main (workers-typed) pass too.

import type { ApprovalStatus } from '../approval-api/types.js';

const STATUS_TIPS: Record<ApprovalStatus, string> = {
  pending:
    'Awaiting a decision and unclaimed. Any actor with the reviewer or admin role can claim or decide it.',
  claimed:
    'A reviewer marked this request as theirs to review. Claiming is optional bookkeeping — anyone authorized can still decide it.',
  approved:
    'Decided affirmatively. The decision minted a grant bound to this exact suspension and the run resumed server-side.',
  rejected:
    'Decided negatively. No grant was minted; the run resumed server-side to a declined outcome.',
  escalated:
    'Past its SLA deadline and flagged by the sweep. Escalation raises visibility only — the request is still fully decidable.',
};

const METRIC_TIPS = {
  Open: 'Requests still awaiting a decision: pending, claimed, or escalated.',
  'SLA breached':
    'Open requests past their SLA deadline — counted whether or not the 15-minute sweep has marked them escalated yet.',
  Escalations:
    'Requests ever escalated by the sweep, including ones decided since. Escalation never closes a request.',
  Decided: 'Requests resolved either way — approved plus rejected.',
  Approved: 'Decisions that minted a grant bound to their exact suspension.',
  Rejected:
    'Decisions that resumed the run to a declined outcome. No grant was minted.',
  'Avg resolution':
    'Mean time from request creation to decision, across decided requests. Shows — until something has been decided.',
} as const;

export type MetricLabel = keyof typeof METRIC_TIPS;

export const APPROVAL_TIPS = {
  sla: 'Each request carries a decide-by deadline (4 hours here). A cron sweep every 15 minutes marks overdue requests as escalated.',
  priority:
    'low, normal, high, or critical — set when a request is created. Approvals auto-queued by this demo are always normal.',
  claim:
    "Marks you as the reviewer working this request so others don't double-handle it. Optional — deciding does not require a prior claim.",
  delegate:
    'Reassigns the claim to another reviewer. Last write wins by design: it moves a pointer and guards no side effect.',
  grantsOnApprove:
    'Connector ids this approval unlocks — for one resumed leg of this exact suspension only. Approving here never unlocks a later gate.',
  decision:
    'The durable decision record. Approve derived a grant for the suspended leg; reject resumed the run to a declined outcome.',
  status: STATUS_TIPS,
  metrics: METRIC_TIPS,
} as const;
