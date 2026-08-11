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

/**
 * A number var that GATES whether a duty runs at all, rather than tuning one
 * that runs regardless. Absent or empty => undefined, silently (the operator
 * named nothing). Invalid => undefined AND a config-error line.
 *
 * The polarity argument is `boolVar`'s, not `numberVar`'s, and the difference
 * is which direction is unsafe. `numberVar` falls back so maintenance keeps
 * running on a typo'd var — correct when the duty runs either way and the
 * number only tunes it (RUN_RETENTION_DAYS). Here the value decides whether an
 * IRREVERSIBLE delete happens at all, so a fallback would not preserve behavior,
 * it would INVENT it — and invent a threshold the caller has already decided it
 * cannot pick on the operator's behalf. Never expiring is recoverable; deleting
 * a deployment's conversations because a var was mistyped is not.
 *
 * The empty-string case is not hypothetical: `''` is what an unset CI/CD
 * variable interpolates to and what a blank wrangler `vars` entry produces, and
 * `numberVar` already reads it as unset — so a caller gating on
 * `raw !== undefined` would silently enable the duty at the fallback value.
 */
export function optionalNumberVar(
  raw: string | undefined,
  name: string,
  options: NumberVarOptions = {},
): number | undefined {
  if (raw === undefined || raw === '') return undefined;
  const value = Number(raw);
  const belowFloor = options.allowZero ? value < 0 : value <= 0;
  if (!Number.isFinite(value) || belowFloor) {
    console.error(
      JSON.stringify({ type: 'config-error', var: name, raw, skipped: true }),
    );
    return undefined;
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

/**
 * Self-decision (separation-of-duties) exemption from an env var. Empty/absent
 * or a `false` spelling => OFF (SoD stays on — the safe default). A `true`
 * spelling => every decider may self-decide. A comma-separated role list
 * (e.g. `admin` or `admin,reviewer`) => only those roles. ANY unrecognized
 * token, or an empty list after splitting, logs a config-error and falls back
 * to OFF — same fail-closed reading `boolVar` gives a garbled kill switch:
 * a mistyped exemption must not silently widen who can self-approve.
 *
 * `validRoles` is passed in (rather than importing the approval-api role set)
 * to keep this module dependency-free; callers pass APPROVAL_ROLES.
 */
export function selfDecisionPolicyVar(
  raw: string | undefined,
  name: string,
  validRoles: readonly string[],
): boolean | { roles: string[] } {
  if (raw === undefined || raw === '') return false;
  const value = raw.trim().toLowerCase();
  if (TRUTHY.has(value)) return true;
  if (FALSY.has(value)) return false;
  const tokens = value
    .split(',')
    .map((token) => token.trim())
    .filter(Boolean);
  const roles = tokens.filter((token) => validRoles.includes(token));
  if (roles.length === 0 || roles.length !== tokens.length) {
    console.error(
      JSON.stringify({
        type: 'config-error',
        var: name,
        raw,
        effective: false,
      }),
    );
    return false;
  }
  return { roles };
}
