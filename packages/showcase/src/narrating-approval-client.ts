// ApprovalApiClient that narrates its own mutations. Subclassing (public
// method overrides only) captures DecideResult.resume — the inline resume
// outcome the dashboard hook discards — without changing the hook's API; the
// hook types against ApprovalApiClient, which this IS. If subclassing ever
// proves fragile, the documented fallback is a delegating wrapper exposing
// the same class type.

import type {
  ApprovalDecision,
  ApprovalRecord,
  DecideResult,
} from '@flowsafe/approval-api/types';
import {
  ApprovalApiClient,
  type ApprovalApiClientOptions,
  ApprovalApiError,
} from '@flowsafe/approval-ui/client';
import {
  claimEvent,
  decideDeniedEvent,
  decideEvents,
  delegateEvent,
  type NarrationEvent,
} from '@/narration';

export interface NarratingApprovalClientOptions
  extends ApprovalApiClientOptions {
  narrate: (events: readonly NarrationEvent[]) => void;
}

export class NarratingApprovalClient extends ApprovalApiClient {
  readonly #narrate: (events: readonly NarrationEvent[]) => void;

  constructor(options: NarratingApprovalClientOptions) {
    const { narrate, ...clientOptions } = options;
    super(clientOptions);
    this.#narrate = narrate;
  }

  override async decide(
    id: string,
    decision: ApprovalDecision,
    comment?: string,
  ): Promise<DecideResult> {
    try {
      const result = await super.decide(id, decision, comment);
      this.#narrate(decideEvents(result));
      return result;
    } catch (error) {
      if (error instanceof ApprovalApiError && error.status === 403) {
        this.#narrate([decideDeniedEvent(id, error.message)]);
      }
      // The dashboard hook renders the error inline either way.
      throw error;
    }
  }

  override async claim(id: string): Promise<ApprovalRecord> {
    const record = await super.claim(id);
    this.#narrate([claimEvent(record)]);
    return record;
  }

  override async delegate(id: string, to: string): Promise<ApprovalRecord> {
    const record = await super.delegate(id, to);
    this.#narrate([delegateEvent(record)]);
    return record;
  }
}
