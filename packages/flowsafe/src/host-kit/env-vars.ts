// SPDX-License-Identifier: Apache-2.0
// Env-var parsing shared by the Worker hosts (previously copied verbatim
// into each). Fallback-not-fail is the numeric contract: maintenance and
// auth must keep running on a typo'd var, and the config-error log line is
// the operator's tripwire. The boolean contract is the opposite where it
// matters: a kill switch fed garbage must KILL, not silently carry on — the
// caller names its fail-closed value.

export interface NumberVarOptions {
  /**
   * Permit 0. Cap-style vars opt in: `DEMO_DAILY_RUN_CAP=0` ("freeze the
   * demo") and `RUN_RETENTION_DAYS=0` ("purge terminal runs immediately")
   * are real operator intents — exactly the values an incident reaches for —
   * and rejecting them silently reverts to the fallback while the
   * config-error line reads as a caught typo. Duration/TTL vars keep the
   * default rejection: a 0-second JWT or 0-hour sandbox is never meant.
   */
  allowZero?: boolean;
}

export function numberVar(
  raw: string | undefined,
  fallback: number,
  name: string,
  options: NumberVarOptions = {},
): number {
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  const belowFloor = options.allowZero ? value < 0 : value <= 0;
  if (!Number.isFinite(value) || belowFloor) {
    // Fall back rather than fail: maintenance must keep running on a typo'd
    // var, and the log line is the operator's tripwire.
    console.error(
      JSON.stringify({ type: 'config-error', var: name, raw, fallback }),
    );
    return fallback;
  }
  return value;
}

const TRUTHY = new Set(['true', '1', 'yes', 'on']);
const FALSY = new Set(['false', '0', 'no', 'off']);

/**
 * Boolean env var. Absent or empty => false (the flag is simply not set).
 * The usual spellings parse case-insensitively (true/1/yes/on,
 * false/0/no/off); any OTHER value logs a config-error and returns
 * `onInvalid` — the caller states its fail-closed reading, because polarity
 * decides it: for a kill switch (DEMO_DISABLED) the safe reading of
 * `DEMO_DISABLED=disable-now-please` is `true` (demo dies), never "carry on
 * as if unset" — an emergency control that only recognizes one literal
 * silently no-ops in the incident it exists for.
 */
export function boolVar(
  raw: string | undefined,
  name: string,
  options: { onInvalid: boolean },
): boolean {
  if (raw === undefined || raw === '') return false;
  const value = raw.trim().toLowerCase();
  if (TRUTHY.has(value)) return true;
  if (FALSY.has(value)) return false;
  console.error(
    JSON.stringify({
      type: 'config-error',
      var: name,
      raw,
      effective: options.onInvalid,
    }),
  );
  return options.onInvalid;
}
