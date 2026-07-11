// SPDX-License-Identifier: Apache-2.0
import { type JSX, useState } from 'react';
import type {
  ApprovalDecision,
  ApprovalRecord,
} from '../approval-api/types.js';
import { OPEN_STATUSES } from '../approval-api/types.js';
import { useApprovalUIComponents } from './components.js';
import { APPROVAL_TIPS } from './tips.js';
import { formatSlaCountdown } from './view-model.js';

export interface DetailViewProps {
  record: ApprovalRecord;
  nowMs: number;
  /** Disables the action controls while a mutation is in flight. */
  busy: boolean;
  onClaim: () => void;
  onDecide: (decision: ApprovalDecision, comment: string) => void;
  onDelegate: (to: string) => void;
}

export function DetailView({
  record,
  nowMs,
  busy,
  onClaim,
  onDecide,
  onDelegate,
}: DetailViewProps): JSX.Element {
  const C = useApprovalUIComponents();
  const [comment, setComment] = useState('');
  const [delegateTo, setDelegateTo] = useState('');
  const open = OPEN_STATUSES.includes(record.status);

  function submitDelegate(): void {
    if (delegateTo !== '') onDelegate(delegateTo);
  }

  return (
    <C.Section aria-label={`Approval ${record.title}`}>
      <C.Stack direction="vertical" gap="md">
        <C.Heading level={2}>{record.title}</C.Heading>
        <C.MetadataList>
          <C.MetadataItem label="Run">
            {record.workflowId}/{record.runId}
            {record.stepPath ? ` @ ${record.stepPath.join('.')}` : null}
          </C.MetadataItem>
          <C.MetadataItem label="Status">
            <C.InfoTip
              label={record.status}
              tip={APPROVAL_TIPS.status[record.status]}
            />
            {record.claimedBy ? ` (claimed by ${record.claimedBy})` : null}
          </C.MetadataItem>
          <C.MetadataItem label="Priority">
            <C.InfoTip label={record.priority} tip={APPROVAL_TIPS.priority} />
          </C.MetadataItem>
          <C.MetadataItem label="SLA">
            <C.InfoTip
              label={formatSlaCountdown(record, nowMs)}
              tip={APPROVAL_TIPS.sla}
            />
          </C.MetadataItem>
          <C.MetadataItem label="Grants on approve">
            <C.InfoTip
              label={
                record.connectors.length > 0
                  ? record.connectors.join(', ')
                  : '—'
              }
              tip={APPROVAL_TIPS.grantsOnApprove}
            />
          </C.MetadataItem>
          {record.decision ? (
            <C.MetadataItem label="Decision">
              <C.InfoTip label={record.decision} tip={APPROVAL_TIPS.decision} />{' '}
              by {record.decidedBy}
              {record.comment ? ` · ${record.comment}` : null}
            </C.MetadataItem>
          ) : null}
        </C.MetadataList>

        {record.summary ? <C.Text>{record.summary}</C.Text> : null}
        {record.payload !== undefined ? (
          <C.Code
            code={JSON.stringify(record.payload, null, 2)}
            language="json"
          />
        ) : null}

        {open ? (
          <C.Stack direction="vertical" gap="sm">
            <C.TextField
              label="Comment"
              value={comment}
              onChange={setComment}
              rows={2}
              disabled={busy}
            />
            <C.Stack direction="horizontal" gap="sm">
              <C.Button
                label="Approve"
                variant="primary"
                disabled={busy}
                onClick={() => onDecide('approve', comment)}
              />
              <C.Button
                label="Reject"
                variant="danger"
                disabled={busy}
                onClick={() => onDecide('reject', comment)}
              />
              <C.Button
                label="Claim"
                variant="secondary"
                disabled={busy}
                onClick={onClaim}
              />
            </C.Stack>
            <C.Stack direction="horizontal" gap="sm">
              <C.TextField
                label="Delegate to"
                value={delegateTo}
                onChange={setDelegateTo}
                placeholder="reviewer id"
                disabled={busy}
                onSubmit={submitDelegate}
              />
              <C.Button
                label="Delegate"
                variant="secondary"
                disabled={busy || delegateTo === ''}
                onClick={submitDelegate}
              />
            </C.Stack>
            <C.Text>
              <C.InfoTip
                label="What does claiming do?"
                tip={APPROVAL_TIPS.claim}
              />
              {' · '}
              <C.InfoTip
                label="What does delegating do?"
                tip={APPROVAL_TIPS.delegate}
              />
            </C.Text>
          </C.Stack>
        ) : null}
      </C.Stack>
    </C.Section>
  );
}
