// SPDX-License-Identifier: Apache-2.0
// This module imports only types from ./contracts.js; runtime collaborators arrive as parameters.
// Nothing at module scope calls an imported binding.

import { z } from 'zod';
import { type AuditEvent, AuditLogger } from '../audit/index.js';
import {
  type ConnectorDecisionCode,
  type ConnectorDenialCode,
  ConnectorInvocationError,
  ConnectorPolicyError,
  ConnectorValidationError,
  captureConnectorDenialMetadata,
  isConnectorDecisionCode,
} from '../connector-decision.js';
import {
  assertEgressHostList,
  egressDomainAllowed,
} from '../policy-engine/tool-policy.js';
import type {
  Connector,
  ConnectorEgressPosture,
  ConnectorInvocationOptions,
  PermissionManifest,
} from './contracts.js';
import type { EgressFetchBase, EgressResponse } from './egress-fetch.js';
import { resolveEgressPosture } from './egress-posture.js';

export interface ConnectorConformanceCase<TInput = unknown> {
  readonly name: string;
  readonly input: TInput;
  readonly invocation?: ConnectorInvocationOptions;
  /** Milliseconds before the case is abandoned as never-settling. Default 2000. */
  readonly timeoutMs?: number;
  readonly respond?: (
    request: ConnectorConformanceRequest,
  ) => ConnectorConformanceResponse; // default: 200, empty body
  readonly expect:
    | { readonly outcome: 'guarded-request'; readonly hosts: readonly string[] }
    | { readonly outcome: 'guarded-denial'; readonly code: ConnectorDenialCode }
    | {
        readonly outcome: 'policy-denied';
        readonly code: ConnectorDecisionCode;
      }
    | { readonly outcome: 'no-network' };
}

export type ConnectorConformanceFindingCode =
  | 'NETWORK_IO_OUTSIDE_RUNTIME_FETCH'
  | 'MANIFEST_MISMATCH'
  | 'POSTURE_NOT_ENFORCED'
  | 'SUBJECT_UNREGISTERED'
  | 'CASE_EXPECTATION_UNMET'
  | 'CASE_INVOCATION_FAILED'
  | 'CASE_TIMEOUT'
  | 'NO_TRANSPORT_EVIDENCE'
  | 'POLICIES_NOT_WIRED'
  | 'FACTORY_FAILED'
  | 'INSTRUMENTATION_UNSUPPORTED'
  | 'INSTRUMENTATION_REPLACED'
  | 'INSTRUMENTATION_NOT_RESTORED'
  | 'RUN_OVERLAPPING'
  | 'ISOLATE_POISONED'
  | 'NO_CASES';

export interface ConnectorConformanceEscape {
  /** Instrumented label, or 'policies.fetch'; never an argument value. */
  readonly entryPoint: string;
  /** Hostname only; null when the argument yielded no parseable URL. */
  readonly host: string | null;
  /** Every recorded escape was refused; the record is written before the refusal. */
  readonly refused: true;
}

export interface ConnectorConformanceFinding {
  readonly code: ConnectorConformanceFindingCode;
  /** Case name, absent for a run-level finding. */
  readonly case?: string;
  /** Which supplied policy member was not wired; POLICIES_NOT_WIRED only. */
  readonly member?: 'fetch' | 'audit' | 'both';
  /**
   * The settled case whose abandoned work produced this run-level finding.
   * Present only on a finding observed after that case settled, where `case`
   * is absent because the case's own result is already on the report.
   */
  readonly observedAfterCase?: string;
  readonly reason: string;
}

export interface ConnectorConformanceCaseResult {
  readonly name: string;
  /**
   * What this case proved. `'nothing'` indicates incomplete evidence; inspect
   * the findings for a refusal, timeout, instrumentation replacement, or
   * invocation failure, including a failure during invocation setup.
   */
  readonly proved:
    | 'guarded-request'
    | 'guarded-denial'
    | 'policy-denied'
    | 'no-network'
    | 'nothing';
  /**
   * Hosts this case reached THROUGH the guard: the hostnames of the calls the
   * harness-owned base transport allowed, de-duplicated in first-call order.
   * Filled when the case was invoked, did not time out, and verified its
   * instrumentation intact at settlement; empty otherwise.
   * A measurement, never a copy of the case's own `expect.hosts`.
   */
  readonly guardedHosts: readonly string[];
  /** Refused attempts recorded for this case; an escape recorded at run level is on `report.findings` instead. */
  readonly escapes: readonly ConnectorConformanceEscape[];
  /**
   * The `decisionCode` of every event this case's AuditLogger recorded inside
   * the invocation window, in record order; `undefined` for an event from an
   * emitter that stamps none, so a foreign boundary writing to the same logger
   * stays visible rather than being filtered away. Filled under the
   * same condition as `guardedHosts`; empty otherwise.
   */
  readonly decisionCodes: readonly (ConnectorDecisionCode | undefined)[];
  /**
   * Calls that reached the harness-owned base transport, allowed or refused,
   * counted when the case settles.
   */
  readonly transportCalls: number;
  /**
   * Witness events on this case's AuditLogger: recorded inside the invocation
   * window, carrying a decisionCode, and stamped with this case's SUBJECT as
   * `resource` — a collaborator connector built on the same logger is not one.
   */
  readonly auditEvents: number;
  /**
   * This case's findings: the subset of report.findings whose `case` is this
   * name. Case names are unique per run, so the subset is well defined.
   */
  readonly findings: readonly ConnectorConformanceFinding[];
}

export interface ConnectorConformanceReport {
  readonly conformant: boolean;
  /**
   * Absent whenever no subject's posture was resolved; the run's findings say
   * why.
   */
  readonly posture?: ConnectorEgressPosture;
  /**
   * Entry points that completed a case's install. A case contributes NONE
   * unless every entry's transaction completed: a failure at ANY entry rolls back
   * the whole stack, including the failing entry on a (d) write or (e) verification
   * failure; an (a) validation or descriptor-read failure precedes capture and push,
   * so the stack holds only entries attempted before it, and neither failure
   * path verifies the entry afterwards.
   * Labels are unique per run. Empty when no case ran.
   */
  readonly instrumented: readonly string[];
  readonly cases: readonly ConnectorConformanceCaseResult[];
  /**
   * Every finding in the run, flat: run-level ones with `case` absent,
   * case-scoped ones carrying the case name. Each case's own view of the same
   * objects is on its ConnectorConformanceCaseResult.
   */
  readonly findings: readonly ConnectorConformanceFinding[];
  /** The finite-case limitation this report does not exceed. The module constant, on every report a run produces, refusals included — `refuseRun` sets it too. */
  readonly limit: string;
}

export interface ConnectorConformanceRuntime {
  /** Wire both members into the connector's policies; the harness owns both. */
  readonly policies: {
    readonly fetch: EgressFetchBase;
    readonly audit: AuditLogger;
  };
}

export type ConnectorConformanceFactory<TInput, TOutput> = (
  runtime: ConnectorConformanceRuntime,
) => Connector<TInput, TOutput>;

export interface ConnectorConformanceRequest {
  /** The exact href the transport was called with; the consumer's own request. */
  readonly url: string;
  readonly host: string;
  readonly method: string;
}

export interface ConnectorConformanceResponse {
  readonly status?: number;
  readonly headers?: Readonly<Record<string, string>>;
  readonly body?: string;
}

export interface ConnectorConformanceEntryPoint {
  readonly label: string;
  readonly target: object;
  readonly property: string;
}

export interface ConnectorConformanceOptions<TInput = unknown> {
  readonly manifest: PermissionManifest;
  readonly cases: readonly ConnectorConformanceCase<TInput>[];
  readonly entryPoints?: readonly ConnectorConformanceEntryPoint[];
}

export class ConnectorConformanceError extends Error {
  readonly kind = 'connector-conformance';
  readonly report: ConnectorConformanceReport;

  constructor(report: ConnectorConformanceReport) {
    super(
      `connector conformance failed: ${
        report.findings.map((finding) => finding.code).join(', ') ||
        'no findings'
      }`,
    );
    this.name = 'ConnectorConformanceError';
    this.report = report;
  }
}

export const CONFORMANCE_LIMIT =
  'conformance covers only the supplied cases, in this isolate, for the duration of each case; channels a run observes, and channels it does not, are described under Conformance limits in the CONNECTORS.md that ships with this package, at https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md#conformance-limits';

class ConformanceRefusal extends Error {}

/** The label of the harness-owned global fetch entry point. */
const GLOBAL_FETCH_LABEL = 'globalThis.fetch';
/** The base transport the harness hands the factory for its policies. */
const POLICIES_FETCH_LABEL = 'policies.fetch';

/** Milliseconds a case runs before it is abandoned as never-settling. */
const DEFAULT_CASE_TIMEOUT_MS = 2000;

/**
 * The run holding this module instance; undefined while it accepts one. This
 * guard and `poisonedByCase` are scoped to this module instance, so a second
 * copy of this module carries its own pair and neither sees the other's runs.
 * The package's `.` and `./connector-sdk` entry points resolve to one copy;
 * `scripts/packed-consumer-test.mjs` asserts that identity against the packed
 * tarball.
 */
let activeRun: symbol | undefined;
/** The case whose timeout poisoned this isolate; undefined while it accepts runs. */
let poisonedByCase: string | undefined;

/** Refuse before a report exists: the findings stand on a synthetic one. */
const refuseRun = (findings: readonly ConnectorConformanceFinding[]): never => {
  throw new ConnectorConformanceError({
    conformant: false,
    instrumented: [],
    cases: [],
    findings,
    limit: CONFORMANCE_LIMIT,
  });
};

/**
 * A destination that changes when the phase it belongs to ends. The transport
 * and the traps a phase hands to connector code hold this object, so where a
 * call arriving after that phase is recorded is a property of the object
 * rather than of a variable every recorder has to consult.
 */
interface PhaseSink<T> {
  readonly record: (value: T) => void;
  readonly close: () => void;
}

function phaseSink<T>(
  open: (value: T) => void,
  late: (value: T) => void,
): PhaseSink<T> {
  let closed = false;
  return {
    record: (value) => {
      if (closed) {
        late(value);
        return;
      }
      open(value);
    },
    close: () => {
      closed = true;
    },
  };
}

interface TimerGlobals {
  setTimeout(handler: () => void, timeoutMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

function globalTimers(): TimerGlobals {
  return globalThis as unknown as TimerGlobals;
}

interface UrlLike {
  readonly href: string;
  readonly hostname: string;
}

type UrlConstructor = new (input: string) => UrlLike;

type TextEncoderConstructor = new () => {
  encode(input: string): Uint8Array<ArrayBuffer>;
};

function requireGlobal<T>(name: string): T {
  const ctor = (globalThis as Record<string, unknown>)[name];
  if (typeof ctor !== 'function') {
    throw new TypeError(
      `assertConnectorConformance requires the ${name} global (Workers, Node >= 18, or a browser)`,
    );
  }
  return ctor as T;
}

/**
 * The recorders take their host from here, and a record is what makes a refusal
 * observable, so this answers `null` rather than throwing — including when the
 * URL global the run required at its start is no longer a constructor.
 */
function urlOf(input: unknown): UrlLike | null {
  try {
    const UrlCtor = requireGlobal<UrlConstructor>('URL');
    if (typeof input === 'string') return new UrlCtor(input);
    if (typeof input !== 'object' || input === null) return null;
    const candidate = input as { href?: unknown; url?: unknown };
    if (typeof candidate.href === 'string') return new UrlCtor(candidate.href);
    if (typeof candidate.url === 'string') return new UrlCtor(candidate.url);
    return null;
  } catch {
    return null;
  }
}

function hostOf(input: unknown): string | null {
  return urlOf(input)?.hostname ?? null;
}

function hrefOf(input: unknown): string | null {
  return urlOf(input)?.href ?? null;
}

function trap(
  entryPoint: string,
  escapes: PhaseSink<ConnectorConformanceEscape>,
) {
  return (...args: readonly unknown[]): never => {
    escapes.record({ entryPoint, host: hostOf(args[0]), refused: true });
    throw new ConformanceRefusal(`connector reached ${entryPoint}`);
  };
}

function buildResponse(
  url: string,
  host: string,
  method: string,
  respond: ConnectorConformanceCase['respond'],
): EgressResponse {
  const response = respond?.({ url, host, method });
  const status = response?.status ?? 200;
  const body = response?.body ?? '';
  const headers = new Map(
    Object.entries(response?.headers ?? {}).map(([name, value]) => [
      name.toLowerCase(),
      value,
    ]),
  );
  return {
    status,
    ok: status >= 200 && status < 300,
    statusText: '',
    url,
    headers: { get: (name) => headers.get(name.toLowerCase()) ?? null },
    json: async () => JSON.parse(body),
    text: async () => body,
    arrayBuffer: async () => {
      const Encoder = requireGlobal<TextEncoderConstructor>('TextEncoder');
      return new Encoder().encode(body).buffer;
    },
  };
}

interface CaseTransport {
  readonly fetch: EgressFetchBase;
  bind(connector: object): void;
  readonly calls: () => number;
  readonly hosts: () => readonly string[];
}

function createCaseTransport(
  connectorManifest: (tool: object) => PermissionManifest | undefined,
  respond:
    | ((request: ConnectorConformanceRequest) => ConnectorConformanceResponse)
    | undefined,
  escapes: PhaseSink<ConnectorConformanceEscape>,
): CaseTransport {
  let subject: object | undefined;
  let calls = 0;
  const allowed: string[] = [];
  // The refusal must reach a synchronous factory caller as a throw.
  const fetch = ((input: unknown, init?: { readonly method?: string }) => {
    calls += 1;
    const url = hrefOf(input);
    const host = hostOf(input);
    const declared =
      subject === undefined ? undefined : connectorManifest(subject)?.egress;
    if (
      url === null ||
      host === null ||
      declared === undefined ||
      !egressDomainAllowed(host, declared)
    ) {
      escapes.record({
        entryPoint: POLICIES_FETCH_LABEL,
        host,
        refused: true,
      });
      throw new ConformanceRefusal(
        'connector called the supplied base transport directly',
      );
    }
    allowed.push(host);
    return Promise.resolve(
      buildResponse(url, host, (init?.method ?? 'GET').toUpperCase(), respond),
    );
  }) as EgressFetchBase;
  return {
    fetch,
    bind: (connector) => {
      subject = connector;
    },
    calls: () => calls,
    hosts: () => [...allowed],
  };
}

function validateOptions<TInput>(
  options: ConnectorConformanceOptions<TInput>,
): ConnectorConformanceOptions<TInput> {
  const object = z.custom<object>(
    (value) => typeof value === 'object' && value !== null,
    'must be an object',
  );
  const code = z.custom<ConnectorDecisionCode>(
    isConnectorDecisionCode,
    'must be a connector decision code',
  );
  // `connector-decision.ts` publishes the denial-code set through this
  // `@internal` capture function's throw path and through no guard of its own,
  // so a code it rejects is one this schema rejects.
  const denialCode = z.custom<ConnectorDenialCode>((value) => {
    try {
      captureConnectorDenialMetadata({ code: value });
      return true;
    } catch {
      return false;
    }
  }, 'must be a connector denial code');
  const caseSchema = z.strictObject({
    name: z.string().min(1),
    input: z.unknown(),
    invocation: object.optional(),
    timeoutMs: z.number().int().positive().max(2_147_483_647).optional(),
    respond: z
      .custom<ConnectorConformanceCase['respond']>(
        (value) => typeof value === 'function',
        'must be a response function',
      )
      .optional(),
    expect: z.discriminatedUnion('outcome', [
      z.strictObject({
        outcome: z.literal('guarded-request'),
        hosts: z.array(z.string()).min(1),
      }),
      z.strictObject({
        outcome: z.literal('guarded-denial'),
        code: denialCode,
      }),
      z.strictObject({ outcome: z.literal('policy-denied'), code }),
      z.strictObject({ outcome: z.literal('no-network') }),
    ]),
  });
  const schema = z.strictObject({
    manifest: object,
    cases: z.array(
      z
        .unknown()
        .refine(
          (value) =>
            typeof value !== 'object' ||
            value === null ||
            Object.hasOwn(value, 'input'),
          { path: ['input'], message: 'required' },
        )
        .pipe(caseSchema),
    ),
    entryPoints: z
      .array(
        z.strictObject({
          label: z.string().min(1),
          target: object,
          property: z.string().min(1),
        }),
      )
      .optional(),
  });
  let result: ReturnType<typeof schema.safeParse>;
  try {
    result = schema.safeParse(options);
  } catch {
    throw new TypeError(
      'assertConnectorConformance: options could not be read or validated',
    );
  }
  if (!result.success) {
    const issue = result.error.issues[0];
    const path = issue?.path.length ? issue.path.join('.') : 'options';
    throw new TypeError(
      `assertConnectorConformance: invalid ${path}: ${issue?.message ?? 'configuration'}`,
    );
  }
  const names = new Set<string>();
  result.data.cases.forEach((c, index) => {
    if (c.expect.outcome === 'guarded-request') {
      assertEgressHostList(
        c.expect.hosts,
        (entry) =>
          `assertConnectorConformance: invalid cases.${index}.expect.hosts: '${entry}' must be a bare hostname ('api.example.com') or wildcard ('*.example.com')`,
      );
    }
    if (names.has(c.name)) {
      throw new TypeError(
        `assertConnectorConformance: invalid cases.${index}.name: duplicate case name`,
      );
    }
    names.add(c.name);
  });
  const labels = new Set<string>([GLOBAL_FETCH_LABEL, POLICIES_FETCH_LABEL]);
  result.data.entryPoints?.forEach((entry, index) => {
    if (labels.has(entry.label)) {
      throw new TypeError(
        `assertConnectorConformance: invalid entryPoints.${index}.label: duplicate or reserved label`,
      );
    }
    labels.add(entry.label);
  });
  // Zod copies case and entry metadata; custom and unknown schemas retain fixture identity.
  return result.data as ConnectorConformanceOptions<TInput>;
}

/**
 * Every member of `PermissionManifest`. The `Record` makes the list a
 * compile-time obligation: a member the interface gains is a missing property
 * here, and a name it does not have is an excess one.
 */
const MANIFEST_MEMBER_SET: Record<keyof PermissionManifest, true> = {
  sideEffect: true,
  egress: true,
  idempotencyKey: true,
  requiresApproval: true,
  dryRun: true,
  rateLimit: true,
  background: true,
  requiredPermissions: true,
  egressEnforcement: true,
};

const MANIFEST_MEMBERS = Object.keys(
  MANIFEST_MEMBER_SET,
) as readonly (keyof PermissionManifest)[];

function manifestsMatch(
  claimed: PermissionManifest,
  registered: PermissionManifest | undefined,
): boolean {
  if (registered === undefined) return false;
  for (const manifest of [claimed, registered]) {
    for (const [key, value] of Object.entries(manifest)) {
      if (
        !MANIFEST_MEMBERS.some((member) => member === key) &&
        value !== undefined
      ) {
        return false;
      }
    }
  }
  return MANIFEST_MEMBERS.every((key) => {
    if (key === 'egress' || key === 'requiredPermissions') {
      const left = claimed[key] ?? (key === 'egress' ? [] : undefined);
      const right = registered[key] ?? (key === 'egress' ? [] : undefined);
      return left === undefined || right === undefined
        ? left === right
        : Array.isArray(left) &&
            Array.isArray(right) &&
            left.length === right.length &&
            left.every((value, index) => Object.is(value, right[index]));
    }
    if (key === 'egressEnforcement') {
      return resolveEgressPosture(claimed) === resolveEgressPosture(registered);
    }
    return Object.is(claimed[key] ?? undefined, registered[key] ?? undefined);
  });
}

interface CapturedEntry extends ConnectorConformanceEntryPoint {
  readonly descriptor: PropertyDescriptor | undefined;
  readonly trap: ReturnType<typeof trap>;
  readonly installedDescriptor: PropertyDescriptor;
}

/** The outcome of one install attempt over a set of entry points. */
interface InstalledEntries {
  /** The installed entries, or undefined when the install failed and unwound. */
  readonly stack: readonly CapturedEntry[] | undefined;
  /** Labels the subject assigned over the install; one finding per label. */
  readonly assigned: ReadonlySet<string>;
  /** Whether the entries this install put back matched their descriptors. */
  readonly restored: boolean;
}

type RecordFinding = (finding: ConnectorConformanceFinding) => void;

const instrumentable = (d: PropertyDescriptor | undefined): boolean =>
  d === undefined ||
  ('value' in d && (d.configurable === true || d.writable === true));

const firstInherited = (target: object, property: string) => {
  let o = Object.getPrototypeOf(target);
  while (o !== null) {
    const d = Object.getOwnPropertyDescriptor(o, property);
    if (d !== undefined) return d;
    o = Object.getPrototypeOf(o);
  }
  return undefined;
};

function describeDescriptor(d: PropertyDescriptor | undefined): string {
  if (d === undefined) return 'absent property';
  if (!('value' in d)) return `accessor (configurable: ${d.configurable})`;
  return `data property (writable: ${d.writable}, configurable: ${d.configurable})`;
}

/**
 * The vocabulary for a value a consumer's own code threw: the error's message
 * where it reads as one, otherwise what `CONNECTORS.md` records under
 * `FACTORY_FAILED`. A function is described by type, because its string form is
 * its source text.
 * `describeValue` is the other vocabulary, for a value the subject threw.
 */
function errorMessage(error: unknown): string {
  try {
    if (error instanceof Error) {
      const message = error.message;
      return typeof message === 'string' ? message : 'unreadable error';
    }
    return typeof error === 'string'
      ? error
      : typeof error === 'object'
        ? error === null
          ? 'null'
          : 'a non-Error object'
        : typeof error === 'function'
          ? describeValue(error)
          : String(error);
  } catch {
    return 'unreadable error';
  }
}

/**
 * The connector owns `constructor.name`, so the reason carries it only when it
 * is a plain identifier of at most 64 characters. Anything else joins the
 * unavailable and unreadable names as `unknown`.
 */
const CONSTRUCTOR_NAME_PATTERN = /^[A-Za-z_$][A-Za-z0-9_$]{0,63}$/;

function errorConstructorName(value: unknown): string {
  try {
    const name = Object.getPrototypeOf(value)?.constructor?.name;
    return typeof name === 'string' && CONSTRUCTOR_NAME_PATTERN.test(name)
      ? name
      : 'unknown';
  } catch {
    return 'unknown';
  }
}

/**
 * Which kind of failure a subject's thrown value is, for a value the harness
 * does not own: each `instanceof` walks a prototype chain the connector can
 * trap, so the whole classification sits inside one `try` and a value that
 * cannot say what it is takes the `foreign` branch. Prefer this where the
 * answer is a classification over several constructors; prefer `isInstanceOf`
 * for a single constructor, and `readProperty` for one property of such a
 * value.
 */
function classifyInvocationError(
  value: unknown,
): 'boundary' | 'policy' | 'refusal' | 'foreign' {
  try {
    if (
      value instanceof ConnectorValidationError ||
      value instanceof ConnectorInvocationError
    ) {
      return 'boundary';
    }
    if (value instanceof ConnectorPolicyError) return 'policy';
    if (value instanceof ConformanceRefusal) return 'refusal';
    return 'foreign';
  } catch {
    return 'foreign';
  }
}

function invocationFailureReason(error: unknown): string {
  try {
    return `case invocation failed with ${error instanceof Error ? errorConstructorName(error) : describeValue(error)}`;
  } catch {
    return 'case invocation failed with unknown';
  }
}

/**
 * The vocabulary for a value the SUBJECT threw or returned: its type, and
 * nothing of the value itself. A reason built from it carries no byte the
 * subject supplied.
 */
function describeValue(value: unknown): string {
  if (value === undefined) return 'undefined';
  if (value === null) return 'null';
  return `${typeof value === 'object' ? 'an' : 'a'} ${typeof value}`;
}

function unregisteredReason(value: unknown): string {
  return `the factory returned ${describeValue(value)} that this copy of createConnector() did not build${
    typeof value === 'object' && value !== null
      ? ': a plain Mastra tool, or a connector from a second copy of the package'
      : ''
  }`;
}

/** Calls after a replacement the harness knows the trap did not serve. */
const CALLS_UNOBSERVED = '; calls made after the replacement were not observed';
/** Calls after a replacement the harness did not determine either way. */
const CALLS_UNCHECKED =
  '; whether calls made after the replacement reached the trap was not checked';

function verifyEntries(
  stack: readonly CapturedEntry[],
  record: RecordFinding,
): boolean {
  let intact = true;
  for (const entry of stack) {
    const { target, property, label, trap: installedTrap } = entry;
    let shape: string;
    let difference = 'descriptor differs from the one the harness installed';
    // Empty where a data property still holds the installed trap, so calls
    // kept reaching it, and on the catch arm, where the descriptor or the
    // effective value could not be read.
    let calls = '';
    try {
      const descriptor = Object.getOwnPropertyDescriptor(target, property);
      shape = describeDescriptor(descriptor);
      if (holdsInstalledDescriptor(entry, descriptor)) {
        const effective = (target as Record<string, unknown>)[property];
        if (effective === installedTrap) continue;
        difference = 'effective value differs from the installed trap';
        calls = CALLS_UNOBSERVED;
        shape += ` resolving to ${describeValue(effective)}`;
      } else if (descriptor !== undefined && 'value' in descriptor) {
        if (descriptor.value !== installedTrap) calls = CALLS_UNOBSERVED;
      } else if (descriptor !== undefined) {
        // An accessor answers each read itself, so the descriptor says nothing
        // about what a call after the replacement reached, and the harness does
        // not invoke a consumer's getter to find out.
        calls = CALLS_UNCHECKED;
      } else {
        // The entry point is gone, so a read after the replacement resolves
        // through the prototype chain or to undefined, and not to the trap.
        calls = CALLS_UNOBSERVED;
      }
    } catch {
      difference = 'descriptor or effective value could not be verified';
      shape = 'an unreadable property or own descriptor';
    }
    intact = false;
    record({
      code: 'INSTRUMENTATION_REPLACED',
      reason: `${label} ${difference}: ${shape}${calls}`,
    });
  }
  return intact;
}

function holdsInstalledDescriptor(
  entry: CapturedEntry,
  descriptor: PropertyDescriptor | undefined,
): boolean {
  const installed = entry.installedDescriptor;
  return (
    descriptor !== undefined &&
    descriptor.enumerable === installed.enumerable &&
    descriptor.configurable === installed.configurable &&
    ('value' in installed
      ? 'value' in descriptor &&
        descriptor.value === entry.trap &&
        descriptor.writable === installed.writable
      : !('value' in descriptor) &&
        descriptor.get === installed.get &&
        descriptor.set === installed.set)
  );
}

function restoreEntries(
  stack: readonly CapturedEntry[],
  record: RecordFinding,
): boolean {
  const failures: { label: string; error: unknown }[] = [];
  for (const { target, property, descriptor, label } of [...stack].reverse()) {
    try {
      if (descriptor !== undefined) {
        Object.defineProperty(target, property, descriptor);
      } else {
        delete (target as Record<string, unknown>)[property];
      }
      const back = Object.getOwnPropertyDescriptor(target, property);
      const same =
        descriptor !== undefined
          ? back !== undefined &&
            'value' in back &&
            Object.is(back.value, descriptor.value) &&
            back.writable === descriptor.writable &&
            back.enumerable === descriptor.enumerable &&
            back.configurable === descriptor.configurable
          : back === undefined;
      if (!same)
        throw new Error(
          'restored descriptor differs from the captured descriptor',
        );
    } catch (error) {
      failures.push({ label, error });
    }
  }
  for (const { label, error } of failures) {
    try {
      record({
        code: 'INSTRUMENTATION_NOT_RESTORED',
        reason: `assertConnectorConformance could not restore ${label}: ${errorMessage(error)}`,
      });
    } catch {
      // Diagnostics cannot interrupt restoration or the caller's timer cleanup.
    }
  }
  return failures.length === 0;
}

function installEntries(
  entries: readonly ConnectorConformanceEntryPoint[],
  escapes: PhaseSink<ConnectorConformanceEscape>,
  record: RecordFinding,
  installer: 'case' | 'probe factory',
): InstalledEntries {
  const stack: CapturedEntry[] = [];
  const assigned = new Set<string>();
  for (const entry of entries) {
    const { target, property, label } = entry;
    const completed = stack.length;
    try {
      const descriptor = Object.getOwnPropertyDescriptor(target, property);
      if (!instrumentable(descriptor)) {
        throw new Error(
          `${describeDescriptor(descriptor)} (a data property that is configurable or writable is required)`,
        );
      }
      if (descriptor === undefined) {
        const inherited = firstInherited(target, property);
        if (inherited !== undefined && !('value' in inherited)) {
          throw new Error('inherited accessor');
        }
      }
      const replacement = trap(label, escapes);
      const installedDescriptor: PropertyDescriptor =
        descriptor === undefined || descriptor.configurable === true
          ? {
              get: () => replacement,
              set: () => {
                if (assigned.has(label)) return;
                assigned.add(label);
                record({
                  code: 'INSTRUMENTATION_REPLACED',
                  reason: `the ${installer} assigned ${label} during execution; the assignment was not applied and the trap was kept`,
                });
              },
              enumerable: descriptor?.enumerable ?? true,
              configurable: true,
            }
          : { ...descriptor, value: replacement };
      const captured = {
        ...entry,
        descriptor,
        trap: replacement,
        installedDescriptor,
      };
      // A mediated write can mutate the target before it throws.
      stack.push(captured);
      if ('value' in installedDescriptor) {
        (target as Record<string, unknown>)[property] = replacement;
      } else {
        Object.defineProperty(target, property, installedDescriptor);
      }
      const d = Object.getOwnPropertyDescriptor(target, property);
      if (!holdsInstalledDescriptor(captured, d)) {
        throw new Error(
          'post-install own descriptor differs from the trap descriptor',
        );
      }
      if ((target as Record<string, unknown>)[property] !== replacement) {
        throw new Error(
          'effective-read mismatch: property does not resolve to the trap',
        );
      }
    } catch (error) {
      verifyEntries(stack.slice(0, completed), record);
      const restored = restoreEntries(stack, record);
      record({
        code: 'INSTRUMENTATION_UNSUPPORTED',
        reason: `assertConnectorConformance cannot instrument ${label}: ${errorMessage(error)}`,
      });
      return { stack: undefined, assigned, restored };
    }
  }
  return { stack, assigned, restored: true };
}

function raceTimeout(
  invocation: Promise<unknown>,
  timeoutMs: number,
  onTimeout: () => void,
  captureTimer: (handle: unknown) => void,
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = globalTimers().setTimeout(() => {
      if (settled) return;
      settled = true;
      onTimeout();
      reject(new ConformanceRefusal('case timed out'));
    }, timeoutMs);
    captureTimer(timer);
    invocation.then(
      (value) => {
        if (settled) return;
        settled = true;
        resolve(value);
      },
      (error: unknown) => {
        if (settled) return;
        settled = true;
        reject(error);
      },
    );
  });
}

function escapeFinding(
  attempt: ConnectorConformanceEscape,
): ConnectorConformanceFinding {
  return {
    code: 'NETWORK_IO_OUTSIDE_RUNTIME_FETCH',
    // A call the supplied base transport refused reached the harness's own
    // transport rather than going around it; CONNECTORS.md states the
    // conditions it refuses on. A call on any other entry point reached an
    // instrument the connector was not given, which is the bypass.
    reason:
      attempt.entryPoint === POLICIES_FETCH_LABEL
        ? `connector reached ${attempt.entryPoint} for a host the registered egress declaration does not cover (host: ${attempt.host ?? 'unparseable'})`
        : `connector reached ${attempt.entryPoint} outside runtime.fetch (host: ${attempt.host ?? 'unparseable'})`,
  };
}

/**
 * The same finding at run level, naming the phase that had already ended when
 * it arrived. A finding recorded here belongs to no case: the case result it
 * would have joined is already on the report. `settledCase` carries that case's
 * name as a field beside the reason, for a caller reading the report by machine;
 * the probe phase ends under no case name and passes none.
 */
function observedAfter(
  finding: ConnectorConformanceFinding,
  phase: string,
  settledCase?: string,
): ConnectorConformanceFinding {
  return {
    ...finding,
    ...(settledCase === undefined ? {} : { observedAfterCase: settledCase }),
    reason: `${finding.reason}; observed after ${phase}`,
  };
}

/**
 * A value a connector registry can hold as a key. `createConnector()`
 * registers the tool it returns, so a value that cannot be a key is one no
 * registry answers for.
 */
function registrySubject(value: unknown): object | undefined {
  return (typeof value === 'object' && value !== null) ||
    typeof value === 'function'
    ? (value as object)
    : undefined;
}

/** What a case proved, and the hosts the classification read. */
interface CaseObservation {
  readonly proved: ConnectorConformanceCaseResult['proved'];
  readonly guardedHosts: readonly string[];
}

/** Everything an eligible case measured, as the classification reads it. */
interface CaseEvidence {
  readonly expect: ConnectorConformanceCase['expect'];
  /** The registered egress declaration, read once for the run. */
  readonly declaredEgress: readonly string[];
  readonly escapes: readonly ConnectorConformanceEscape[];
  readonly witnesses: readonly AuditEvent[];
  readonly transportCalls: number;
  readonly transportHosts: readonly string[];
  /** Present when the invocation threw; the value is inside it. */
  readonly invocation: { readonly thrown: unknown } | undefined;
}

/**
 * Classify a case that was invoked, kept its instrumentation and settled in
 * time. It reads measurements and records findings; it performs no I/O and
 * holds no state between cases.
 */
function observeCase(
  evidence: CaseEvidence,
  record: RecordFinding,
): CaseObservation {
  const guardedHosts = [...new Set(evidence.transportHosts)];
  // A value whose classification cannot be read is by definition none of the
  // three known kinds, so it takes the foreign branch.
  const invocationKind = classifyInvocationError(evidence.invocation?.thrown);
  const boundaryError = invocationKind === 'boundary';
  if (evidence.invocation !== undefined && invocationKind === 'foreign') {
    record({
      code: 'CASE_INVOCATION_FAILED',
      reason: invocationFailureReason(evidence.invocation.thrown),
    });
  }
  const missingFetch =
    evidence.transportCalls === 0 &&
    evidence.escapes.some(
      (attempt) =>
        attempt.entryPoint === GLOBAL_FETCH_LABEL &&
        attempt.host !== null &&
        egressDomainAllowed(attempt.host, evidence.declaredEgress),
    );
  const missingAudit = !boundaryError && evidence.witnesses.length === 0;
  if (missingFetch || missingAudit) {
    record({
      code: 'POLICIES_NOT_WIRED',
      member: missingFetch ? (missingAudit ? 'both' : 'fetch') : 'audit',
      reason: missingFetch
        ? 'either the factory did not wire policies.fetch, or the connector called the ambient global directly for a host it declares; the escape record beside this finding is authoritative for the request itself.' +
          (missingAudit
            ? ' The subject recorded no audit witness on the supplied logger.'
            : '')
        : // A failed invocation is evidence the boundary was NOT reached, so
          // the absent witness is what that failure left behind, not a wiring
          // conclusion the run can draw.
          evidence.invocation !== undefined
          ? 'the invocation failed before the subject could record an audit witness on the supplied logger'
          : 'the subject reached its gate boundary but recorded no audit witness on the supplied logger; wire policies.audit',
    });
  }
  if (boundaryError && evidence.witnesses.length === 0) {
    record({
      code: 'CASE_EXPECTATION_UNMET',
      reason:
        "the case produced no audit event because the connector's gate boundary was never reached; a pre-boundary refusal is not expressible by any expectation and belongs in an ordinary connector test",
    });
    return { proved: 'nothing', guardedHosts };
  }
  const proved = evidence.witnesses.some(
    (event) =>
      event.decision === 'denied' && event.policyKind === 'egress-fetch',
  )
    ? 'guarded-denial'
    : evidence.witnesses.some(
          (event) =>
            event.decision === 'denied' && event.policyKind !== 'egress-fetch',
        )
      ? 'policy-denied'
      : guardedHosts.length > 0
        ? 'guarded-request'
        : 'no-network';
  const expected = evidence.expect;
  const evidenceMatches =
    expected.outcome === 'guarded-request'
      ? expected.hosts.every((host) =>
          guardedHosts.some((actual) => egressDomainAllowed(actual, [host])),
        )
      : expected.outcome === 'no-network' ||
        evidence.witnesses.some(
          (event) => event.decisionCode === expected.code,
        );
  if (proved !== expected.outcome || !evidenceMatches) {
    record({
      code: 'CASE_EXPECTATION_UNMET',
      reason: `case expected ${expected.outcome} but proved ${proved}, or its required hosts or code were not observed`,
    });
  }
  return { proved, guardedHosts };
}

export function createConformanceAssertion(collaborators: {
  connectorManifest: (tool: object) => PermissionManifest | undefined;
  connectorEgressPosture: (tool: object) => ConnectorEgressPosture | undefined;
  invokeConnector: <TInput, TOutput>(
    connector: Connector<TInput, TOutput>,
    input: TInput,
    options?: ConnectorInvocationOptions,
  ) => Promise<TOutput>;
}) {
  const { connectorManifest, connectorEgressPosture, invokeConnector } =
    collaborators;
  return async function assertConnectorConformance<TInput, TOutput>(
    factory: ConnectorConformanceFactory<TInput, TOutput>,
    rawOptions: ConnectorConformanceOptions<TInput>,
  ): Promise<ConnectorConformanceReport> {
    const normalized = validateOptions(rawOptions);
    requireGlobal<TimerGlobals['setTimeout']>('setTimeout');
    requireGlobal<TimerGlobals['clearTimeout']>('clearTimeout');
    requireGlobal<UrlConstructor>('URL');
    requireGlobal<TextEncoderConstructor>('TextEncoder');
    const globalEntry = {
      label: GLOBAL_FETCH_LABEL,
      target: globalThis,
      property: 'fetch',
    };
    const entries = [globalEntry, ...(normalized.entryPoints ?? [])];
    for (const [index, entry] of entries.entries()) {
      const previous = entries
        .slice(0, index)
        .find(
          (other) =>
            other.target === entry.target && other.property === entry.property,
        );
      if (previous !== undefined) {
        refuseRun([
          {
            code: 'INSTRUMENTATION_UNSUPPORTED',
            reason: `${previous.label} and ${entry.label} name the same target and property`,
          },
        ]);
      }
    }
    if (activeRun !== undefined) {
      refuseRun([
        {
          code: 'RUN_OVERLAPPING',
          reason: 'another conformance run is active in this isolate',
        },
      ]);
    }
    if (poisonedByCase !== undefined) {
      refuseRun([
        {
          code: 'ISOLATE_POISONED',
          reason: `case '${poisonedByCase}' timed out in this isolate; no further run is accepted`,
        },
      ]);
    }
    activeRun = Symbol();
    const findings: ConnectorConformanceFinding[] = [];
    const cases: ConnectorConformanceCaseResult[] = [];
    const instrumented = new Set<string>();
    let posture: ConnectorEgressPosture | undefined;
    let runClosed = false;
    const recordRun: RecordFinding = (finding) => {
      // The run is closed and the report the caller holds is fixed, so this
      // finding is recorded nowhere: CONFORMANCE_LIMIT covers a run for the
      // duration of its own cases.
      if (runClosed) return;
      findings.push(finding);
    };
    /**
     * Close the run and take the findings its report delivers: the flag is
     * set here, and what the report carries is a copy the recorders can no
     * longer reach.
     */
    const closeRun = (): readonly ConnectorConformanceFinding[] => {
      runClosed = true;
      return [...findings];
    };
    try {
      const probeEscapes = phaseSink<ConnectorConformanceEscape>(
        (attempt) => {
          recordRun(escapeFinding(attempt));
        },
        (attempt) => {
          recordRun(
            observedAfter(escapeFinding(attempt), 'the probe factory returned'),
          );
        },
      );
      const probeInstall = installEntries(
        [globalEntry],
        probeEscapes,
        recordRun,
        'probe factory',
      );
      const probeStack = probeInstall.stack;
      if (probeStack === undefined) return refuseRun(closeRun());
      let probe: unknown;
      let factoryFailed = false;
      let probeIntact = true;
      let probeRestored = true;
      try {
        const probeTransport = createCaseTransport(
          connectorManifest,
          undefined,
          probeEscapes,
        );
        const probeLogger = new AuditLogger({
          // The probe reads no event. The sink is what makes the logger an
          // exporting one, which a factory's own policies can require.
          sink: () => {},
        });
        probe = factory({
          policies: Object.freeze({
            fetch: probeTransport.fetch,
            audit: probeLogger,
          }),
        });
      } catch (error) {
        factoryFailed = true;
        recordRun({ code: 'FACTORY_FAILED', reason: errorMessage(error) });
      } finally {
        probeIntact = verifyEntries(probeStack, recordRun);
        probeRestored = restoreEntries(probeStack, recordRun);
      }
      probeEscapes.close();
      if (
        factoryFailed ||
        !probeIntact ||
        !probeRestored ||
        probeInstall.assigned.size > 0
      ) {
        return refuseRun(closeRun());
      }
      const probeSubject = registrySubject(probe);
      posture =
        probeSubject === undefined
          ? undefined
          : connectorEgressPosture(probeSubject);
      const probeManifest =
        probeSubject === undefined
          ? undefined
          : connectorManifest(probeSubject);
      if (posture === undefined || probeManifest === undefined) {
        recordRun({
          code: 'SUBJECT_UNREGISTERED',
          reason: unregisteredReason(probe),
        });
      } else {
        if (posture !== 'enforced') {
          recordRun({
            code: 'POSTURE_NOT_ENFORCED',
            reason: 'the connector declares a declaration-only egress posture',
          });
        }
        // One read of the claimed manifest's members: a getter cannot answer
        // one way for this comparison and another way afterwards.
        const claimedManifest = { ...normalized.manifest };
        if (!manifestsMatch(claimedManifest, probeManifest)) {
          recordRun({
            code: 'MANIFEST_MISMATCH',
            reason: 'the registered manifest differs from the claimed manifest',
          });
        }
        if (posture === 'enforced') {
          // The registered egress declaration, read once for the whole run.
          const declaredEgress = probeManifest.egress ?? [];
          if (normalized.cases.length === 0) {
            recordRun({
              code: 'NO_CASES',
              reason: 'an empty case set supplies no conformance evidence',
            });
          }
          let invokedAny = false;
          let stopped = false;
          for (const [index, c] of normalized.cases.entries()) {
            const caseName = c.name;
            const settled = `case '${caseName}' settled`;
            const escapes: ConnectorConformanceEscape[] = [];
            const caseFindings: ConnectorConformanceFinding[] = [];
            const escapeSink = phaseSink<ConnectorConformanceEscape>(
              (attempt) => {
                escapes.push(attempt);
              },
              (attempt) => {
                recordRun(
                  observedAfter(escapeFinding(attempt), settled, caseName),
                );
              },
            );
            const findingSink = phaseSink<ConnectorConformanceFinding>(
              (finding) => {
                const scoped = { ...finding, case: caseName };
                recordRun(scoped);
                caseFindings.push(scoped);
              },
              (finding) => {
                recordRun(observedAfter(finding, settled, caseName));
              },
            );
            const recordCase: RecordFinding = findingSink.record;
            const caseInstall = installEntries(
              entries,
              escapeSink,
              recordCase,
              'case',
            );
            let caseTransport: CaseTransport | undefined;
            const caseAuditEvents: {
              readonly event: AuditEvent;
              readonly inWindow: boolean;
            }[] = [];
            let inWindow = false;
            let subjectId: string | undefined;
            const isWitness = (e: { event: AuditEvent; inWindow: boolean }) =>
              e.inWindow &&
              e.event.decisionCode !== undefined &&
              e.event.resource === subjectId;
            let invoked = false;
            let timedOut = false;
            let instrumentationIntact = true;
            let invocation: { readonly thrown: unknown } | undefined;
            let restoredAfterCase = true;
            let timer: unknown;
            const caseStack = caseInstall.stack;
            if (caseStack !== undefined) {
              for (const entry of caseStack) instrumented.add(entry.label);
              try {
                caseTransport = createCaseTransport(
                  connectorManifest,
                  c.respond,
                  escapeSink,
                );
                const caseLogger = new AuditLogger({
                  sink: (event) => {
                    try {
                      // The witness set holds the harness's own copy of every
                      // event, never the object the connector still holds.
                      caseAuditEvents.push({ event: { ...event }, inWindow });
                    } catch {
                      // An event that cannot be copied leaves no witness, so
                      // the case reports absent wiring rather than accepting
                      // evidence the harness could not read.
                    }
                  },
                });
                let produced: { readonly value: unknown } | undefined;
                try {
                  produced = {
                    value: factory({
                      policies: Object.freeze({
                        fetch: caseTransport.fetch,
                        audit: caseLogger,
                      }),
                    }),
                  };
                } catch (error) {
                  recordCase({
                    code: 'FACTORY_FAILED',
                    reason: errorMessage(error),
                  });
                }
                if (produced !== undefined) {
                  const caseSubject = registrySubject(produced.value);
                  const casePosture =
                    caseSubject === undefined
                      ? undefined
                      : connectorEgressPosture(caseSubject);
                  const caseManifest =
                    caseSubject === undefined
                      ? undefined
                      : connectorManifest(caseSubject);
                  if (
                    caseSubject === undefined ||
                    casePosture === undefined ||
                    caseManifest === undefined
                  ) {
                    recordCase({
                      code: 'SUBJECT_UNREGISTERED',
                      reason: unregisteredReason(produced.value),
                    });
                  } else if (casePosture !== posture) {
                    recordCase({
                      code: 'POSTURE_NOT_ENFORCED',
                      reason: 'the case subject posture differs from the probe',
                    });
                  } else if (!manifestsMatch(probeManifest, caseManifest)) {
                    recordCase({
                      code: 'MANIFEST_MISMATCH',
                      reason:
                        'the case subject manifest differs from the probe',
                    });
                  } else {
                    // The registry answered for this subject, so it is a
                    // connector createConnector() built.
                    const connector = caseSubject as Connector<TInput, TOutput>;
                    caseTransport.bind(connector);
                    subjectId = connector.id;
                    invoked = true;
                    invokedAny = true;
                    inWindow = true;
                    await raceTimeout(
                      invokeConnector(connector, c.input, c.invocation),
                      c.timeoutMs ?? DEFAULT_CASE_TIMEOUT_MS,
                      () => {
                        timedOut = true;
                      },
                      (handle) => {
                        timer = handle;
                      },
                    );
                  }
                }
              } catch (error) {
                if (timedOut) {
                  poisonedByCase = caseName;
                  recordCase({
                    code: 'CASE_TIMEOUT',
                    reason: `case '${caseName}' timed out after ${c.timeoutMs ?? DEFAULT_CASE_TIMEOUT_MS} ms`,
                  });
                } else {
                  invocation = { thrown: error };
                }
              } finally {
                inWindow = false;
                instrumentationIntact = verifyEntries(caseStack, recordCase);
                restoredAfterCase = restoreEntries(caseStack, recordCase);
                if (timer !== undefined) {
                  try {
                    globalTimers().clearTimeout(timer);
                  } catch {
                    // The settled race ignores a timer whose cleanup fails.
                  }
                }
              }
            }
            const caseEscapes = [...escapes];
            escapeSink.close();
            for (const attempt of caseEscapes)
              recordCase(escapeFinding(attempt));
            if (
              !invoked &&
              invocation !== undefined &&
              // A refusal is the harness's own throw: the attempt behind it is
              // already a NETWORK_IO_OUTSIDE_RUNTIME_FETCH finding, and the
              // sentinel names a class no consumer can see. The invoked path
              // excludes it through the same classification.
              classifyInvocationError(invocation.thrown) !== 'refusal'
            ) {
              recordCase({
                code: 'CASE_INVOCATION_FAILED',
                reason: invocationFailureReason(invocation.thrown),
              });
            }
            const witnesses = caseAuditEvents
              .filter(isWitness)
              .map((e) => e.event);
            const decisionCodes = caseAuditEvents
              .filter((e) => e.inWindow)
              .map((e) => e.event.decisionCode);
            const transportCalls = caseTransport?.calls() ?? 0;
            // An invocation failure under a replaced instrument or a timeout
            // raises no CASE_INVOCATION_FAILED of its own: the case is
            // ineligible, and the INSTRUMENTATION_REPLACED or CASE_TIMEOUT
            // finding beside it is why it proves nothing.
            const eligible = invoked && !timedOut && instrumentationIntact;
            const observation: CaseObservation = eligible
              ? observeCase(
                  {
                    expect: c.expect,
                    declaredEgress,
                    escapes: caseEscapes,
                    witnesses,
                    transportCalls,
                    transportHosts: caseTransport?.hosts() ?? [],
                    invocation,
                  },
                  recordCase,
                )
              : { proved: 'nothing', guardedHosts: [] };
            cases.push({
              name: caseName,
              proved: observation.proved,
              guardedHosts: observation.guardedHosts,
              escapes: caseEscapes,
              decisionCodes: eligible ? decisionCodes : [],
              transportCalls,
              auditEvents: witnesses.length,
              findings: [...caseFindings],
            });
            findingSink.close();
            const restorationFailed =
              !caseInstall.restored || !restoredAfterCase;
            const stoppingCode = timedOut
              ? 'CASE_TIMEOUT'
              : restorationFailed
                ? 'INSTRUMENTATION_NOT_RESTORED'
                : undefined;
            if (stoppingCode !== undefined) {
              const remaining = normalized.cases
                .slice(index + 1)
                .map((remainingCase) => remainingCase.name);
              if (remaining.length > 0) {
                recordRun({
                  code: stoppingCode,
                  reason: `skipped cases ${remaining.join(', ')} after ${stoppingCode} in '${caseName}'`,
                });
              }
              stopped = true;
              break;
            }
          }
          if (
            invokedAny &&
            !stopped &&
            declaredEgress.length > 0 &&
            !cases.some((c) => c.transportCalls > 0)
          ) {
            recordRun({
              code: 'NO_TRANSPORT_EVIDENCE',
              reason:
                'no case reached the harness transport for the registered egress declaration',
            });
          }
        }
      }
    } finally {
      activeRun = undefined;
    }
    const runFindings = closeRun();
    const report: ConnectorConformanceReport = {
      conformant: runFindings.length === 0,
      ...(posture === undefined ? {} : { posture }),
      instrumented: [...instrumented],
      cases: [...cases],
      findings: runFindings,
      limit: CONFORMANCE_LIMIT,
    };
    if (!report.conformant) throw new ConnectorConformanceError(report);
    return report;
  };
}
