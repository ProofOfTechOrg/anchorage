// SPDX-License-Identifier: Apache-2.0

/**
 * Pending notifications whose delivery or summary time has arrived.
 *
 * Bind two positional parameters to the same ISO "now": `deliverAt` first,
 * then `summaryAt`.
 */
export const DUE_NOTIFICATION_SQL =
  "status = 'pending' AND ((deliverAt IS NOT NULL AND deliverAt <= ?) OR (summaryAt IS NOT NULL AND summaryAt <= ?))";
