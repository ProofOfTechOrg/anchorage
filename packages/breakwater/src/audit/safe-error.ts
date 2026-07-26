// SPDX-License-Identifier: Apache-2.0

export interface SafeAuditErrorSummary {
  reason: string;
  detail?: Readonly<Record<string, string | number | boolean>>;
}

const summaries = new WeakMap<object, SafeAuditErrorSummary>();

export function registerSafeAuditError<T extends object>(
  error: T,
  summary: SafeAuditErrorSummary,
): T {
  summaries.set(error, {
    reason: summary.reason,
    detail: summary.detail ? { ...summary.detail } : undefined,
  });
  return error;
}

export function safeAuditErrorSummary(
  error: unknown,
): SafeAuditErrorSummary | undefined {
  if (typeof error !== 'object' || error === null) return undefined;
  return summaries.get(error);
}
