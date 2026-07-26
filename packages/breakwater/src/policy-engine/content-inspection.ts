// SPDX-License-Identifier: Apache-2.0
// Content inspection — agent-boundary PolicyEvaluators layered on the same
// contract as denyPatterns/maxTextLength (index.ts): piiSecrets scans gated
// text for PII/secrets via bounded regex + entropy + Luhn detectors;
// classifierPolicy is a pluggable async-classifier seam (e.g. a moderation
// model call) evaluated on a streaming cadence. Both are best-effort — see
// each export's doc for its accepted evasion surface.

import type { OutputChannel, PolicyEvaluator, PolicyPhase } from './index.js';
import type { PolicyDecision } from './tool-policy.js';

// ---------------------------------------------------------------------------
// piiSecrets — regex + entropy + Luhn detectors
// ---------------------------------------------------------------------------

const EMAIL_RE = /[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9.-]{1,253}\.[A-Za-z]{2,24}/g;
const SSN_RE = /\b\d{3}-\d{2}-\d{4}\b/g;
const PHONE_INTL_RE = /\+\d{7,15}\b/g;
const PHONE_LOCAL_RE = /\(?\d{3}\)?[-. ]\d{3}[-. ]\d{4}\b/g;
const CREDIT_CARD_CANDIDATE_RE = /\b(?:\d[ -]?){12,18}\d\b/g;
const AWS_ACCESS_KEY_RE = /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/g;
const PRIVATE_KEY_RE = /-----BEGIN [A-Z0-9 ]{0,32}PRIVATE KEY-----/g;
const JWT_RE =
  /\beyJ[A-Za-z0-9_-]{8,256}\.[A-Za-z0-9_-]{8,256}\.[A-Za-z0-9_-]{8,256}\b/g;
const SECRET_ASSIGNMENT_RE =
  /(?:api[_-]?key|secret|token|passw(?:or)?d|credential)["']?\s*[:=]\s*["']?[A-Za-z0-9+/_=-]{12,128}/gi;
// DEFAULT-threshold candidate matcher (zero-alloc fast path). Floor 23 is
// candidateFloorForThreshold(DEFAULT_ENTROPY_THRESHOLD): the shortest length
// whose maximum Shannon entropy log2(23) ~= 4.52 reaches the 4.5 bits/char
// default bar (log2(20..22) = 4.32..4.46 < 4.5 — those lengths can never fire
// at 4.5, the dead zone the old {20} floor used to match). A caller-configured
// threshold derives its OWN floor in highEntropyDetector (a 4.0 bar reaches
// back down to floor 20, a 5.0 bar up to 32), so this constant is the default
// only — see candidateFloorForThreshold. Exported only for the white-box
// candidate-floor invariant test — not part of the public API.
export const HIGH_ENTROPY_CANDIDATE_RE = /[A-Za-z0-9+/_=-]{23,256}/g;

// secretAssignment's `\s*` around the separator has no regex-level upper
// bound; for the streaming rescan window (see holdBackChars below) a
// realistic whitespace run is treated as capped at 8 chars each — a longer
// pathological run could evade the incremental scan, the same accepted
// best-effort posture as the evasion caveat on piiSecrets itself.
const SECRET_ASSIGNMENT_MAX_SPAN = 10 + 1 + 8 + 1 + 8 + 1 + 128; // 157

/** Every piiSecrets detector id, in the default scan order — the domain of PiiSecretsOptions.detectors. */
export const PII_SECRETS_DETECTOR_IDS = [
  'email',
  'ssn',
  'phone',
  'creditCard',
  'awsAccessKey',
  'privateKey',
  'jwt',
  'secretAssignment',
  'highEntropy',
] as const;

/** Identifier accepted by {@link PiiSecretsOptions.detectors}. */
export type PiiSecretsDetectorId = (typeof PII_SECRETS_DETECTOR_IDS)[number];

const DEFAULT_ENTROPY_THRESHOLD = 4.5;
const LEAK_PREVENTION_CHANNELS: readonly OutputChannel[] = [
  'answer',
  'reasoning',
  'object',
];

interface DetectorMatch {
  /** Matched text — used only for allowlist/Luhn/entropy checks; never surfaced in a decision reason. */
  text: string;
  index: number;
}

interface Detector {
  id: PiiSecretsDetectorId;
  /** Upper bound on this detector's match length; sizes the streaming rescan window. */
  maxSpan: number;
  scan(window: string): DetectorMatch[];
}

/**
 * Runs a shared, module-level 'g'-flagged RegExp over `window`, returning
 * every match's text + start offset. lastIndex is reset before and after so
 * the next call (or a concurrent detector reusing the same constant) never
 * inherits stale scan state — safe because each call's loop runs to
 * completion synchronously, with no `await` in between.
 */
function matchAll(window: string, pattern: RegExp): DetectorMatch[] {
  pattern.lastIndex = 0;
  const matches: DetectorMatch[] = [];
  for (let m = pattern.exec(window); m !== null; m = pattern.exec(window)) {
    matches.push({ text: m[0], index: m.index });
    // Defensive: none of this module's patterns can match empty, but a
    // shared iteration helper should not be able to spin forever if reused
    // with one that could.
    if (m[0].length === 0) pattern.lastIndex += 1;
  }
  pattern.lastIndex = 0;
  return matches;
}

function luhnValid(digits: string): boolean {
  let sum = 0;
  let double = false;
  for (let i = digits.length - 1; i >= 0; i--) {
    let d = digits.charCodeAt(i) - 48;
    if (double) {
      d *= 2;
      if (d > 9) d -= 9;
    }
    sum += d;
    double = !double;
  }
  return sum % 10 === 0;
}

function shannonEntropy(text: string): number {
  const counts = new Map<string, number>();
  for (const ch of text) counts.set(ch, (counts.get(ch) ?? 0) + 1);
  let entropy = 0;
  for (const count of counts.values()) {
    const p = count / text.length;
    entropy -= p * Math.log2(p);
  }
  return entropy;
}

// A candidate "looks like" a secret rather than prose/identifiers when it
// carries a digit AND either mixed case or base64 punctuation — cheap gates
// that filter out most English text before the entropy computation runs.
function looksLikeSecretShape(text: string): boolean {
  const hasDigit = /\d/.test(text);
  const hasBothCases = /[a-z]/.test(text) && /[A-Z]/.test(text);
  const hasBase64Punct = /[+/=]/.test(text);
  return hasDigit && (hasBothCases || hasBase64Punct);
}

function emailDetector(): Detector {
  return { id: 'email', maxSpan: 343, scan: (w) => matchAll(w, EMAIL_RE) };
}

function ssnDetector(): Detector {
  return { id: 'ssn', maxSpan: 11, scan: (w) => matchAll(w, SSN_RE) };
}

function phoneDetector(): Detector {
  return {
    id: 'phone',
    maxSpan: 16,
    scan: (w) =>
      [...matchAll(w, PHONE_INTL_RE), ...matchAll(w, PHONE_LOCAL_RE)].sort(
        (a, b) => a.index - b.index,
      ),
  };
}

function creditCardDetector(): Detector {
  return {
    id: 'creditCard',
    maxSpan: 37,
    scan(window) {
      const matches: DetectorMatch[] = [];
      for (const candidate of matchAll(window, CREDIT_CARD_CANDIDATE_RE)) {
        const digits = candidate.text.replace(/[ -]/g, '');
        if (digits.length < 13 || digits.length > 19) continue;
        if (!luhnValid(digits)) continue;
        matches.push(candidate);
      }
      return matches;
    },
  };
}

function awsAccessKeyDetector(): Detector {
  return {
    id: 'awsAccessKey',
    maxSpan: 20,
    scan: (w) => matchAll(w, AWS_ACCESS_KEY_RE),
  };
}

function privateKeyDetector(): Detector {
  return {
    id: 'privateKey',
    maxSpan: 59,
    scan: (w) => matchAll(w, PRIVATE_KEY_RE),
  };
}

function jwtDetector(): Detector {
  return { id: 'jwt', maxSpan: 773, scan: (w) => matchAll(w, JWT_RE) };
}

function secretAssignmentDetector(): Detector {
  return {
    id: 'secretAssignment',
    maxSpan: SECRET_ASSIGNMENT_MAX_SPAN,
    scan: (w) => matchAll(w, SECRET_ASSIGNMENT_RE),
  };
}

// Smallest candidate length whose MAXIMUM achievable Shannon entropy — log2(L),
// reached only when every char is distinct — still meets `threshold` bits/char,
// floored at the original baseline 20 so a lowered bar never widens the net
// past it. This is an EXTRACTION floor only; the per-candidate entropy check
// (shannonEntropy >= threshold) is unchanged — deliberately NOT the rejected
// per-candidate clamp min(threshold, log2(len)) (RA-004), which would flag any
// all-distinct run regardless of the configured bar. DL-005 (F5) set the {23}
// floor for the 4.5 default only; hardcoding it silently dropped the 20..22-char
// candidates a lower configured threshold can still legitimately flag
// (4.0 -> floor 20 -> log2(20)=4.32 >= 4.0). Examples:
//   4.5 -> max(20, ceil(2^4.5)=23) = 23   (default; byte-identical to DL-005)
//   4.0 -> max(20, ceil(2^4.0)=16) = 20
//   5.0 -> max(20, ceil(2^5.0)=32) = 32
function candidateFloorForThreshold(threshold: number): number {
  return Math.max(20, Math.ceil(2 ** threshold));
}

function highEntropyDetector(entropyThreshold: number): Detector {
  // Extract candidates at the floor the CONFIGURED threshold makes reachable,
  // not the hardcoded default. The default reuses the shared module RE
  // (zero-alloc fast path); a non-default threshold compiles a one-time RE here
  // at detector construction (never per scan). Clamp the floor to the 256 max
  // span so an out-of-range threshold (floor > 256 — unreachable anyway for a
  // 66-symbol alphabet, max entropy log2(66) ~= 6.04) can't build an
  // out-of-order {min,256} quantifier and throw at construction.
  const floor = candidateFloorForThreshold(entropyThreshold);
  const candidateRe =
    floor === candidateFloorForThreshold(DEFAULT_ENTROPY_THRESHOLD)
      ? HIGH_ENTROPY_CANDIDATE_RE
      : new RegExp(`[A-Za-z0-9+/_=-]{${Math.min(floor, 256)},256}`, 'g');
  return {
    id: 'highEntropy',
    maxSpan: 256,
    scan(window) {
      const matches: DetectorMatch[] = [];
      for (const candidate of matchAll(window, candidateRe)) {
        if (!looksLikeSecretShape(candidate.text)) continue;
        if (shannonEntropy(candidate.text) < entropyThreshold) continue;
        matches.push(candidate);
      }
      return matches;
    },
  };
}

type CompiledAllowlistEntry = string | RegExp;

// Strip g/y flags once, like denyPatterns' pattern compilation: a shared
// evaluator reusing a caller-supplied g/y-flagged RegExp across many calls
// would let its mutated lastIndex skip matches on alternating calls.
function compileAllowlist(
  entries: readonly (string | RegExp)[],
): readonly CompiledAllowlistEntry[] {
  return entries.map((entry) => {
    if (typeof entry === 'string') return entry.toLowerCase();
    return entry.global || entry.sticky
      ? new RegExp(entry.source, entry.flags.replace(/[gy]/g, ''))
      : entry;
  });
}

function isAllowlisted(
  text: string,
  allowlist: readonly CompiledAllowlistEntry[],
): boolean {
  const lower = text.toLowerCase();
  for (const entry of allowlist) {
    if (typeof entry === 'string' ? lower === entry : entry.test(text)) {
      return true;
    }
  }
  return false;
}

// Denies on the first non-allowlisted match found, scanning detectors in
// their configured order. The reason names the detector id and the match's
// ordinal position within that detector's own results — never the matched
// text (see piiSecrets' doc).
function scanForDenial(
  window: string,
  detectors: readonly Detector[],
  allowlist: readonly CompiledAllowlistEntry[],
): PolicyDecision | undefined {
  for (const detector of detectors) {
    const matches = detector.scan(window);
    for (const [index, match] of matches.entries()) {
      if (isAllowlisted(match.text, allowlist)) continue;
      return {
        allowed: false,
        reason: `${detector.id} detected (match index ${index})`,
      };
    }
  }
  return undefined;
}

/** Configuration for {@link piiSecrets}. */
export interface PiiSecretsOptions {
  /** Policy name used in denials and audit records. */
  name?: string;
  /** Subset of detector ids to run. Default: all. */
  detectors?: readonly PiiSecretsDetectorId[];
  /**
   * Exemptions: a match is skipped when its text equals (case-insensitive) an
   * allowlist string, or tests true against an allowlist RegExp.
   */
  allowlist?: readonly (string | RegExp)[];
  /** Minimum bits/char for the highEntropy detector. Default 4.5. */
  entropyThreshold?: number;
  /** Agent lifecycle phases to inspect. Default: both input and output. */
  phases?: readonly PolicyPhase[];
  /** Default: ['answer', 'reasoning', 'object'] — leak prevention, matching denyPatterns' default. */
  channels?: readonly OutputChannel[];
  /** Override the computed hold-back hint (maxEnabledSpan - 1). */
  holdBackChars?: number;
}

/**
 * Deny when the gated text contains PII or a credential-shaped secret:
 * email, ssn, phone, creditCard (candidate digit runs Luhn-validated —
 * only a valid check digit denies), awsAccessKey, privateKey (PEM header
 * only; never tries to span the whole key body), jwt, secretAssignment
 * (`key: value`-shaped assignments), and highEntropy (Shannon entropy over
 * a base64-shaped candidate). Gates all output channels by default, like
 * denyPatterns — a secret is no less leaked through a reasoning trace.
 *
 * Best-effort, like denyPatterns' own evasion caveat: normalization and
 * Unicode tricks (homoglyphs, zero-width characters, alternate encodings)
 * evade the regex detectors, and entropy scoring is a heuristic, not a
 * proof of secrecy — natural text can score high and a deliberately-shaped
 * secret can score low. Do not rely on it alone against adversarial input.
 *
 * Streaming windows are re-sliced from partway through the accumulated text
 * on every call; a slice that starts mid-token can introduce a `\b` word
 * boundary the full text does not have at that position. Accepted,
 * consistent with denyPatterns' own windowing — a spurious boundary can only
 * ever produce an extra scan, never hide a real match, so it fails closed.
 */
export function piiSecrets(options: PiiSecretsOptions = {}): PolicyEvaluator {
  const entropyThreshold =
    options.entropyThreshold ?? DEFAULT_ENTROPY_THRESHOLD;
  const registry: Record<PiiSecretsDetectorId, Detector> = {
    email: emailDetector(),
    ssn: ssnDetector(),
    phone: phoneDetector(),
    creditCard: creditCardDetector(),
    awsAccessKey: awsAccessKeyDetector(),
    privateKey: privateKeyDetector(),
    jwt: jwtDetector(),
    secretAssignment: secretAssignmentDetector(),
    highEntropy: highEntropyDetector(entropyThreshold),
  };
  const detectors = (options.detectors ?? PII_SECRETS_DETECTOR_IDS).map(
    (id) => registry[id],
  );
  const maxEnabledSpan = detectors.reduce(
    (max, detector) => Math.max(max, detector.maxSpan),
    0,
  );
  const allowlist = compileAllowlist(options.allowlist ?? []);
  return {
    name: options.name ?? 'pii-secrets',
    phases: options.phases,
    channels: options.channels ?? LEAK_PREVENTION_CHANNELS,
    holdBackChars: options.holdBackChars ?? Math.max(0, maxEnabledSpan - 1),
    evaluate({ text, channel, streamState }): PolicyDecision {
      // Incremental scan, mirroring denyPatterns' cursor — never for the
      // object channel, whose text is a REPLACED snapshot, not append-only.
      if (streamState && channel !== 'object') {
        const cursorKey = `scannedUpTo:${channel}`;
        const cursor = streamState[cursorKey];
        const scannedUpTo = typeof cursor === 'number' ? cursor : 0;
        const window = text.slice(
          Math.max(0, scannedUpTo - (maxEnabledSpan - 1)),
        );
        const denial = scanForDenial(window, detectors, allowlist);
        if (denial) return denial;
        streamState[cursorKey] = text.length;
        return { allowed: true };
      }
      return scanForDenial(text, detectors, allowlist) ?? { allowed: true };
    },
  };
}

// ---------------------------------------------------------------------------
// classifierPolicy — pluggable async classifier seam
// ---------------------------------------------------------------------------

const DEFAULT_EVALUATE_EVERY_CHARS = 512;
const DEFAULT_CLASSIFIER_CHANNELS: readonly OutputChannel[] = ['answer'];

/** Configuration for {@link classifierPolicy}. */
export interface ClassifierPolicyOptions {
  /** Policy name used in denials and audit records. */
  name?: string;
  /** Async (or sync) classification of `text`; the authoritative decision for this call. */
  classify: (
    text: string,
    info: { phase: PolicyPhase; channel: OutputChannel },
  ) => PolicyDecision | Promise<PolicyDecision>;
  /** Agent lifecycle phases to classify. Default: both input and output. */
  phases?: readonly PolicyPhase[];
  /** Default: ['answer']. */
  channels?: readonly OutputChannel[];
  /** Streaming cadence: classify once accumulated text grows by this many chars. Default 512. */
  evaluateEveryChars?: number;
  /** Optional; a classify call exceeding this throws (fails closed) instead of hanging. */
  timeoutMs?: number;
}

// Structural, unconditional Node/Workers/browser globals — no import, no
// @types/node, no DOM lib needed to typecheck (mirrors agent-cli/index.ts's
// TimerGlobals: setTimeout/clearTimeout are host APIs, not part of the
// ECMAScript lib this package's build compiles against).
interface TimerGlobals {
  setTimeout(handler: () => void, timeoutMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

function globalTimers(): TimerGlobals {
  return globalThis as unknown as TimerGlobals;
}

// Races `decision` against a timer, like agent-cli's exec timeout: a shared
// `settled` guard makes each side a no-op once the other has already won, and
// a rejection handler is always attached to `decision` (via .then's second
// argument) so a late settle after the timer already won is still handled,
// never an unhandled rejection.
function raceTimeout(
  decision: PolicyDecision | Promise<PolicyDecision>,
  timeoutMs: number,
  classifierName: string,
): Promise<PolicyDecision> {
  const timers = globalTimers();
  return new Promise<PolicyDecision>((resolve, reject) => {
    let settled = false;
    const timer = timers.setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(
        new Error(
          `classifier ${classifierName} timed out after ${timeoutMs}ms`,
        ),
      );
    }, timeoutMs);
    Promise.resolve(decision).then(
      (value) => {
        if (settled) return;
        settled = true;
        timers.clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        if (settled) return;
        settled = true;
        timers.clearTimeout(timer);
        reject(error);
      },
    );
  });
}

async function runClassify(
  options: ClassifierPolicyOptions,
  name: string,
  text: string,
  phase: PolicyPhase,
  channel: OutputChannel,
): Promise<PolicyDecision> {
  const decision = options.classify(text, { phase, channel });
  return options.timeoutMs === undefined
    ? decision
    : raceTimeout(decision, options.timeoutMs, name);
}

/**
 * Delegate gating to a pluggable, possibly-async classifier (e.g. a
 * moderation model or an external safety API). Streaming calls `classify`
 * only once accumulated text since the last call has grown by
 * `evaluateEveryChars` (a `classifiedUpTo:${channel}` cursor in
 * `streamState`, per-channel); the object channel classifies every snapshot,
 * since it is a replaced snapshot rather than append-only text. Input and
 * result phases have no `streamState` and therefore always classify — the
 * result phase is the authoritative gate, so a stream whose tail never
 * crossed the cadence still gets classified there.
 *
 * A `timeoutMs` classify call that does not settle in time THROWS (the
 * PolicyEngine's evaluator-crash path audits it and fails closed — aborting
 * in-stream, rethrowing at input/result); fail-open is deliberately not
 * offered. Omit `timeoutMs` to let `classify` run unbounded.
 *
 * The returned evaluator carries no `holdBackChars` hint (engine default:
 * 0) — an async classifier has no bounded straddle window to report, unlike
 * a fixed pattern length. A caller wanting this policy to participate in
 * zero-leak hold-back must accept buffering the ENTIRE channel until stream
 * end, and opt in explicitly: `{ ...classifierPolicy(options), holdBackChars: Infinity }`.
 */
export function classifierPolicy(
  options: ClassifierPolicyOptions,
): PolicyEvaluator {
  const name = options.name ?? 'classifier';
  const evaluateEveryChars =
    options.evaluateEveryChars ?? DEFAULT_EVALUATE_EVERY_CHARS;
  return {
    name,
    phases: options.phases,
    channels: options.channels ?? DEFAULT_CLASSIFIER_CHANNELS,
    evaluate({
      phase,
      channel,
      text,
      streamState,
    }): PolicyDecision | Promise<PolicyDecision> {
      if (streamState && channel !== 'object') {
        const cursorKey = `classifiedUpTo:${channel}`;
        const cursor = streamState[cursorKey];
        const classifiedUpTo = typeof cursor === 'number' ? cursor : 0;
        if (text.length - classifiedUpTo < evaluateEveryChars) {
          return { allowed: true };
        }
        streamState[cursorKey] = text.length;
      }
      return runClassify(options, name, text, phase, channel);
    },
  };
}
