// SPDX-License-Identifier: Apache-2.0

/** Structural producer subset of a Cloudflare `Queue<TEvent>` binding. */
export interface AuditQueue<TEvent = unknown> {
  send(message: TEvent): Promise<unknown>;
}
