/**
 * Maintenance runs on TWO cron expressions, dispatched on controller.cron so
 * the SLA sweep and the retention purge NEVER share an invocation — a
 * CPU-limit kill is uncatchable, so sharing one would let a slow sweep
 * permanently starve the purge. Keep these literals equal to wrangler.jsonc's
 * `triggers.crons`.
 *
 * Own module (not worker.ts) because the e2e test imports them and workerd
 * rejects any entry-module export that is not a handler/class/function — a
 * bare const exported from worker.ts fails the whole Worker at startup.
 */
export const SWEEP_CRON = '*/15 * * * *';
export const PURGE_CRON = '7 * * * *';
