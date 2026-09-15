// SPDX-License-Identifier: Apache-2.0
// This module imports only types from ./index.js; runtime collaborators arrive as parameters.
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
import type { EgressFetchBase, EgressResponse } from './egress-fetch.js';
import type {
  Connector,
  ConnectorEgressPosture,
  ConnectorInvocationOptions,
  PermissionManifest,
} from './index.js';

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
   * A measurement, never a copy of the case's own `expect.hosts` (§5.4 step 22).
   */
  readonly guardedHosts: readonly string[];
  /** Refused attempts recorded for this case; run-level escapes are not here (see below). */
  readonly escapes: readonly ConnectorConformanceEscape[];
  /**
   * The `decisionCode` of every event this case's AuditLogger recorded inside
   * the invocation window, in record order; `undefined` for an event from an
   * emitter that stamps none, so a foreign boundary writing to the same logger
   * stays visible rather than being filtered away (§3.10).
   */
  readonly decisionCodes: readonly (ConnectorDecisionCode | undefined)[];
  /** Calls that reached the harness-owned base transport, allowed or refused (§3.10). */
  readonly transportCalls: number;
  /**
   * Witness events on this case's AuditLogger: recorded inside the invocation
   * window, carrying a decisionCode, and stamped with this case's SUBJECT as
   * `resource` — a collaborator connector built on the same logger is not one
   * (§3.10).
   */
  readonly auditEvents: number;
  /**
   * This case's findings: the subset of report.findings whose `case` is this
   * name. Case names are unique per run (§5.3), so the subset is well defined.
   */
  readonly findings: readonly ConnectorConformanceFinding[];
}

export interface ConnectorConformanceReport {
  readonly conformant: boolean;
  /**
   * Absent whenever no subject's posture was resolved; the run's findings say
   * why. Assigned at step 6 (§5.4).
   */
  readonly posture?: ConnectorEgressPosture;
  /**
   * Entry points that completed a case's install. A case contributes NONE
   * unless every entry's transaction completed: a failure at ANY entry rolls back
   * the whole stack, including the failing entry on a (d) write or (e) verification
   * failure; an (a) validation or descriptor-read failure precedes capture and push,
   * so the stack holds only entries attempted before it, and step 14 never runs
   * for either failure path (§5.4 steps 12-14).
   * Labels are unique per run (§5.3). Empty when no case ran.
   */
  readonly instrumented: readonly string[];
  readonly cases: readonly ConnectorConformanceCaseResult[];
  /**
   * Every finding in the run, flat: run-level ones with `case` absent,
   * case-scoped ones carrying the case name. Each case's own view of the same
   * objects is on its ConnectorConformanceCaseResult.
   */
  readonly findings: readonly ConnectorConformanceFinding[];
  /** The finite-case limitation this report does not exceed. The module constant, on every report a run produces, refusals included — `refuseRun` sets it too (§5.4 step 6, §5.2). */
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
  'conformance covers only the supplied cases, in this isolate, for the duration of each case; the channels it does not observe are listed at https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md#conformance-limits';

class ConformanceRefusal extends Error {}

let activeRun: symbol | undefined;
let isolatePoisoned = false;
let timedOutCase: string | undefined;

const refuseRun = (
  findings: readonly ConnectorConformanceFinding[],
  report?: ConnectorConformanceReport,
): never => {
  throw new ConnectorConformanceError(
    report ?? {
      conformant: false,
      instrumented: [],
      cases: [],
      findings,
      limit: CONFORMANCE_LIMIT,
    },
  );
};

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

function requireGlobal<T>(name: string): T {
  const ctor = (globalThis as Record<string, unknown>)[name];
  if (typeof ctor !== 'function') {
    throw new TypeError(
      `assertConnectorConformance requires the ${name} global (Workers, Node >= 18, or a browser)`,
    );
  }
  return ctor as T;
}

function urlOf(input: unknown): UrlLike | null {
  const UrlCtor = requireGlobal<UrlConstructor>('URL');
  try {
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
  record: (attempt: ConnectorConformanceEscape) => void,
) {
  return (...args: readonly unknown[]): never => {
    record({ entryPoint, host: hostOf(args[0]), refused: true });
    throw new ConformanceRefusal(`connector reached ${entryPoint}`);
  };
}

const hostDeclared = (host: string, declared: readonly string[]): boolean =>
  egressDomainAllowed(host, declared);

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
      const Encoder =
        requireGlobal<
          new () => { encode(input: string): Uint8Array<ArrayBuffer> }
        >('TextEncoder');
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
  record: (attempt: ConnectorConformanceEscape) => void,
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
      !hostDeclared(host, declared)
    ) {
      record({ entryPoint: 'policies.fetch', host, refused: true });
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
  const labels = new Set(['globalThis.fetch', 'policies.fetch']);
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

const MANIFEST_MEMBERS = [
  'sideEffect',
  'egress',
  'idempotencyKey',
  'requiresApproval',
  'dryRun',
  'rateLimit',
  'background',
  'requiredPermissions',
  'egressEnforcement',
] as const;

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
      return (
        (claimed[key] ?? 'declaration-only') ===
        (registered[key] ?? 'declaration-only')
      );
    }
    return Object.is(claimed[key] ?? undefined, registered[key] ?? undefined);
  });
}

interface CapturedEntry extends ConnectorConformanceEntryPoint {
  assignmentRecorded: boolean;
  readonly existed: boolean;
  readonly descriptor: PropertyDescriptor | undefined;
  readonly trap: ReturnType<typeof trap>;
  readonly installedDescriptor: PropertyDescriptor;
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
        : String(error);
  } catch {
    return 'unreadable error';
  }
}

function errorConstructorName(value: unknown): string {
  try {
    const name = Object.getPrototypeOf(value)?.constructor?.name;
    return typeof name === 'string' && name.length > 0 ? name : 'unknown';
  } catch {
    return 'unknown';
  }
}

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

function describeValue(value: unknown): string {
  if (value === undefined) return 'undefined';
  if (value === null) return 'null';
  return `${typeof value === 'object' ? 'an' : 'a'} ${typeof value}`;
}

function unregisteredReason(value: unknown): string {
  return `the factory returned ${describeValue(value)} that createConnector() did not build${
    typeof value === 'object' && value !== null
      ? ': a plain Mastra tool, or a connector from a second copy of the package'
      : ''
  }`;
}

function verifyEntries(
  stack: readonly CapturedEntry[],
  record: RecordFinding,
): boolean {
  let intact = true;
  for (const entry of stack) {
    const { target, property, label, trap: installedTrap } = entry;
    let shape: string;
    let difference = 'descriptor differs from the one the harness installed';
    let callsUnobserved = false;
    try {
      const descriptor = Object.getOwnPropertyDescriptor(target, property);
      shape = describeDescriptor(descriptor);
      if (holdsInstalledDescriptor(entry, descriptor)) {
        const effective = (target as Record<string, unknown>)[property];
        if (effective === installedTrap) continue;
        difference = 'effective value differs from the installed trap';
        callsUnobserved = true;
        shape += ` resolving to ${describeValue(effective)}`;
      } else if (descriptor !== undefined && 'value' in descriptor) {
        callsUnobserved = descriptor.value !== installedTrap;
      }
    } catch {
      difference = 'descriptor or effective value could not be verified';
      shape = 'an unreadable property or own descriptor';
    }
    intact = false;
    record({
      code: 'INSTRUMENTATION_REPLACED',
      reason: `${label} ${difference}: ${shape}${callsUnobserved ? '; calls made after the replacement were not observed' : ''}`,
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
): void {
  const failures: { label: string; error: unknown }[] = [];
  for (const { target, property, existed, descriptor, label } of [
    ...stack,
  ].reverse()) {
    try {
      if (existed && descriptor !== undefined) {
        Object.defineProperty(target, property, descriptor);
      } else {
        delete (target as Record<string, unknown>)[property];
      }
      const back = Object.getOwnPropertyDescriptor(target, property);
      const same =
        existed && descriptor !== undefined
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
}

function installEntries(
  entries: readonly ConnectorConformanceEntryPoint[],
  recordEscape: (attempt: ConnectorConformanceEscape) => void,
  record: RecordFinding,
  subject: 'case' | 'probe factory',
): CapturedEntry[] | undefined {
  const stack: CapturedEntry[] = [];
  for (const entry of entries) {
    const { target, property, label } = entry;
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
      const replacement = trap(label, recordEscape);
      const installedDescriptor: PropertyDescriptor =
        descriptor === undefined || descriptor.configurable === true
          ? {
              get: () => replacement,
              set: () => {
                if (captured.assignmentRecorded) return;
                captured.assignmentRecorded = true;
                record({
                  code: 'INSTRUMENTATION_REPLACED',
                  reason: `the ${subject} assigned ${label} during execution; the assignment was not applied and the trap was kept`,
                });
              },
              enumerable: descriptor?.enumerable ?? true,
              configurable: true,
            }
          : { ...descriptor, value: replacement };
      const captured = {
        ...entry,
        assignmentRecorded: false,
        existed: descriptor !== undefined,
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
      restoreEntries(stack, record);
      record({
        code: 'INSTRUMENTATION_UNSUPPORTED',
        reason: `assertConnectorConformance cannot instrument ${label}: ${errorMessage(error)}`,
      });
      return undefined;
    }
  }
  return stack;
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
    reason: `connector reached ${attempt.entryPoint} outside runtime.fetch (host: ${attempt.host ?? 'unparseable'})`,
  };
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
    options: ConnectorConformanceOptions<TInput>,
  ): Promise<ConnectorConformanceReport> {
    const normalized = validateOptions(options);
    requireGlobal<TimerGlobals['setTimeout']>('setTimeout');
    requireGlobal<TimerGlobals['clearTimeout']>('clearTimeout');
    requireGlobal<UrlConstructor>('URL');
    const globalEntry = {
      label: 'globalThis.fetch',
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
    if (isolatePoisoned) {
      refuseRun([
        {
          code: 'ISOLATE_POISONED',
          reason: `case '${timedOutCase}' timed out in this isolate; no further run is accepted`,
        },
      ]);
    }
    activeRun = Symbol();
    const findings: ConnectorConformanceFinding[] = [];
    const cases: ConnectorConformanceCaseResult[] = [];
    const instrumented = new Set<string>();
    let posture: ConnectorEgressPosture | undefined;
    let runClosed = false;
    try {
      const recordRun: RecordFinding = (finding) => {
        if (runClosed) return;
        findings.push(finding);
      };
      const recordProbeEscape = (attempt: ConnectorConformanceEscape) => {
        recordRun(escapeFinding(attempt));
      };
      const probeStack = installEntries(
        [globalEntry],
        recordProbeEscape,
        recordRun,
        'probe factory',
      );
      if (probeStack === undefined) {
        runClosed = true;
        return refuseRun(findings);
      }
      let probe!: Connector<TInput, TOutput>;
      try {
        const probeTransport = createCaseTransport(
          connectorManifest,
          undefined,
          recordProbeEscape,
        );
        const probeEvents: AuditEvent[] = [];
        const probeLogger = new AuditLogger({
          sink: (event) => {
            probeEvents.push(event);
          },
        });
        probe = factory({
          policies: Object.freeze({
            fetch: probeTransport.fetch,
            audit: probeLogger,
          }),
        });
      } catch (error) {
        recordRun({ code: 'FACTORY_FAILED', reason: errorMessage(error) });
      } finally {
        verifyEntries(probeStack, recordRun);
        restoreEntries(probeStack, recordRun);
      }
      if (
        findings.some(
          (f) =>
            f.code === 'FACTORY_FAILED' ||
            f.code === 'INSTRUMENTATION_REPLACED' ||
            f.code === 'INSTRUMENTATION_NOT_RESTORED',
        )
      ) {
        runClosed = true;
        return refuseRun(findings);
      }
      posture = connectorEgressPosture(probe);
      const probeManifest = connectorManifest(probe);
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
        if (!manifestsMatch(normalized.manifest, probeManifest)) {
          recordRun({
            code: 'MANIFEST_MISMATCH',
            reason: 'the registered manifest differs from the claimed manifest',
          });
        }
        if (posture === 'enforced') {
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
            const escapes: ConnectorConformanceEscape[] = [];
            const caseFindings: ConnectorConformanceFinding[] = [];
            const recordCase: RecordFinding = (finding) => {
              const scoped = { ...finding, case: caseName };
              findings.push(scoped);
              caseFindings.push(scoped);
            };
            let caseSettled = false;
            const recordEscape = (attempt: ConnectorConformanceEscape) => {
              if (runClosed) return;
              if (caseSettled) {
                const finding = escapeFinding(attempt);
                recordRun({
                  ...finding,
                  reason: `${finding.reason}; observed after case '${caseName}' settled`,
                });
                return;
              }
              escapes.push(attempt);
            };
            const stack = installEntries(
              entries,
              recordEscape,
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
            let invocationError: unknown;
            let invocationFailed = false;
            let timer: unknown;
            if (stack !== undefined) {
              for (const entry of stack) instrumented.add(entry.label);
              try {
                caseTransport = createCaseTransport(
                  connectorManifest,
                  c.respond,
                  recordEscape,
                );
                const caseLogger = new AuditLogger({
                  sink: (event) => {
                    try {
                      caseAuditEvents.push({ event: { ...event }, inWindow });
                    } catch {}
                  },
                });
                let connector!: Connector<TInput, TOutput>;
                let factoryReturned = false;
                try {
                  connector = factory({
                    policies: Object.freeze({
                      fetch: caseTransport.fetch,
                      audit: caseLogger,
                    }),
                  });
                  factoryReturned = true;
                } catch (error) {
                  recordCase({
                    code: 'FACTORY_FAILED',
                    reason: errorMessage(error),
                  });
                }
                if (factoryReturned) {
                  const casePosture = connectorEgressPosture(connector);
                  const caseManifest = connectorManifest(connector);
                  if (casePosture === undefined || caseManifest === undefined) {
                    recordCase({
                      code: 'SUBJECT_UNREGISTERED',
                      reason: unregisteredReason(connector),
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
                    caseTransport.bind(connector);
                    subjectId = connector.id;
                    invoked = true;
                    invokedAny = true;
                    inWindow = true;
                    await raceTimeout(
                      invokeConnector(connector, c.input, c.invocation),
                      c.timeoutMs ?? 2000,
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
                  isolatePoisoned = true;
                  timedOutCase = caseName;
                  recordCase({
                    code: 'CASE_TIMEOUT',
                    reason: `case '${caseName}' timed out after ${c.timeoutMs ?? 2000} ms`,
                  });
                } else {
                  invocationError = error;
                  invocationFailed = true;
                }
              } finally {
                inWindow = false;
                instrumentationIntact = verifyEntries(stack, recordCase);
                restoreEntries(stack, recordCase);
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
            caseSettled = true;
            for (const attempt of caseEscapes)
              recordCase(escapeFinding(attempt));
            if (!invoked && invocationFailed) {
              recordCase({
                code: 'CASE_INVOCATION_FAILED',
                reason: invocationFailureReason(invocationError),
              });
            }
            const witnesses = caseAuditEvents
              .filter(isWitness)
              .map((e) => e.event);
            const transportCalls = caseTransport?.calls() ?? 0;
            let proved: ConnectorConformanceCaseResult['proved'] = 'nothing';
            let guardedHosts: string[] = [];
            let decisionCodes: (ConnectorDecisionCode | undefined)[] = [];
            if (invoked && !timedOut && instrumentationIntact) {
              guardedHosts = [...new Set(caseTransport?.hosts() ?? [])];
              decisionCodes = caseAuditEvents
                .filter((e) => e.inWindow)
                .map((e) => e.event.decisionCode);
              // A value whose classification cannot be read is by definition
              // none of the three known kinds, so it takes the foreign branch.
              const invocationKind = classifyInvocationError(invocationError);
              const boundaryError = invocationKind === 'boundary';
              if (invocationFailed && invocationKind === 'foreign') {
                recordCase({
                  code: 'CASE_INVOCATION_FAILED',
                  reason: invocationFailureReason(invocationError),
                });
              }
              const missingFetch =
                transportCalls === 0 &&
                caseEscapes.some(
                  (attempt) =>
                    attempt.entryPoint === 'globalThis.fetch' &&
                    attempt.host !== null &&
                    hostDeclared(attempt.host, probeManifest.egress ?? []),
                );
              const missingAudit = !boundaryError && witnesses.length === 0;
              if (missingFetch || missingAudit) {
                recordCase({
                  code: 'POLICIES_NOT_WIRED',
                  member: missingFetch
                    ? missingAudit
                      ? 'both'
                      : 'fetch'
                    : 'audit',
                  reason: missingFetch
                    ? 'either the factory did not wire policies.fetch, or the connector called the ambient global directly for a host it declares; the escape record beside this finding is authoritative for the request itself.' +
                      (missingAudit
                        ? ' The subject recorded no audit witness on the supplied logger.'
                        : '')
                    : 'the subject reached its gate boundary but recorded no audit witness on the supplied logger; wire policies.audit',
                });
              }
              if (boundaryError && witnesses.length === 0) {
                recordCase({
                  code: 'CASE_EXPECTATION_UNMET',
                  reason:
                    "the case produced no audit event because the connector's gate boundary was never reached; a pre-boundary refusal is not expressible by any expectation and belongs in an ordinary connector test",
                });
              } else {
                proved = witnesses.some(
                  (event) =>
                    event.decision === 'denied' &&
                    event.policyKind === 'egress-fetch',
                )
                  ? 'guarded-denial'
                  : witnesses.some(
                        (event) =>
                          event.decision === 'denied' &&
                          event.policyKind !== 'egress-fetch',
                      )
                    ? 'policy-denied'
                    : guardedHosts.length > 0
                      ? 'guarded-request'
                      : 'no-network';
                const expected = c.expect;
                const evidenceMatches =
                  expected.outcome === 'guarded-request'
                    ? expected.hosts.every((host) =>
                        guardedHosts.some((actual) =>
                          egressDomainAllowed(actual, [host]),
                        ),
                      )
                    : expected.outcome === 'no-network' ||
                      witnesses.some(
                        (event) => event.decisionCode === expected.code,
                      );
                if (proved !== expected.outcome || !evidenceMatches) {
                  recordCase({
                    code: 'CASE_EXPECTATION_UNMET',
                    reason: `case expected ${expected.outcome} but proved ${proved}, or its required hosts or code were not observed`,
                  });
                }
              }
            }
            cases.push({
              name: caseName,
              proved,
              guardedHosts,
              escapes: caseEscapes,
              decisionCodes,
              transportCalls,
              auditEvents: witnesses.length,
              findings: [...caseFindings],
            });
            const stoppingCode = timedOut
              ? 'CASE_TIMEOUT'
              : caseFindings.some(
                    (f) => f.code === 'INSTRUMENTATION_NOT_RESTORED',
                  )
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
            (probeManifest.egress?.length ?? 0) > 0 &&
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
    runClosed = true;
    const runFindings = [...findings];
    const report: ConnectorConformanceReport = {
      conformant: runFindings.length === 0,
      ...(posture === undefined ? {} : { posture }),
      instrumented: [...instrumented],
      cases: [...cases],
      findings: runFindings,
      limit: CONFORMANCE_LIMIT,
    };
    if (!report.conformant) refuseRun(report.findings, report);
    return report;
  };
}
