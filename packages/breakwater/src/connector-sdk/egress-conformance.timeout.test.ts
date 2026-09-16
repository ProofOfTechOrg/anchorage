// SPDX-License-Identifier: Apache-2.0
// A case that times out poisons its module instance: later runs through that
// instance are refused. The obligations that time out live here, in a file and
// a vitest invocation of their own, rather than beside the harness suite.
import { describe, expect, it, vi } from 'vitest';
import {
  assertConnectorConformance,
  type ConnectorConfig,
  type ConnectorConformanceCase,
  ConnectorConformanceError,
  type ConnectorConformanceFactory,
  type ConnectorConformanceReport,
  type ConnectorConformanceRuntime,
  createConnector,
  type PermissionManifest,
} from './index.js';

const manifest: PermissionManifest = {
  sideEffect: 'read',
  egress: ['api.vendor.example'],
  egressEnforcement: 'enforced',
};
const noEgress: PermissionManifest = {
  sideEffect: 'read',
  egressEnforcement: 'enforced',
};
const requestCase: ConnectorConformanceCase = {
  name: 'request',
  input: {},
  expect: { outcome: 'guarded-request', hosts: ['api.vendor.example'] },
};
const quietCase: ConnectorConformanceCase = {
  name: 'quiet',
  input: {},
  expect: { outcome: 'no-network' },
};

type Execute = ConnectorConfig<unknown, unknown>['execute'];
function factory(
  execute: Execute = async (_input, _context, runtime) => {
    await runtime.fetch('https://api.vendor.example');
    return {};
  },
  permissions: PermissionManifest = manifest,
): ConnectorConformanceFactory<unknown, unknown> {
  return (runtime) =>
    createConnector<unknown, unknown>({
      id: 'vendor.read',
      description: 'Conformance timeout fixture',
      permissions,
      policies: runtime.policies,
      execute,
    });
}

function quietFactory(execute: Execute = async () => ({})) {
  return factory(execute, noEgress);
}

async function rejected(
  run: Promise<ConnectorConformanceReport>,
): Promise<ConnectorConformanceReport> {
  try {
    await run;
  } catch (error) {
    expect(error).toBeInstanceOf(ConnectorConformanceError);
    if (error instanceof ConnectorConformanceError) return error.report;
    throw error;
  }
  throw new Error('expected a non-conformant run');
}

describe('connector egress conformance timeouts', () => {
  it('restores instrumentation and poisons the isolate when a case name getter starts throwing after validation and the invocation never settles', async () => {
    // #given
    vi.resetModules();
    const sdk = await import('./index.js');
    const saved = Object.getOwnPropertyDescriptor(globalThis, 'fetch');
    let unreadable = false;
    const name = vi.fn(() => {
      if (unreadable) throw new Error('case name unreadable');
      return 'validated timeout name';
    });
    const subject = (runtime: ConnectorConformanceRuntime) =>
      sdk.createConnector({
        id: 'vendor.timeout',
        description: 'Case name timeout fixture',
        permissions: noEgress,
        policies: runtime.policies,
        execute: async () => {
          unreadable = true;
          Object.defineProperty(globalThis, 'fetch', {
            value: globalThis.fetch,
            writable: true,
            configurable: true,
          });
          return new Promise(() => {});
        },
      });
    // #when
    const error = await sdk
      .assertConnectorConformance(subject, {
        manifest: noEgress,
        cases: [
          {
            ...quietCase,
            get name() {
              return name();
            },
            timeoutMs: 50,
          },
          { ...quietCase, name: 'skipped' },
        ],
      })
      .catch((error: unknown) => error);
    // #then
    expect(error).toBeInstanceOf(sdk.ConnectorConformanceError);
    if (!(error instanceof sdk.ConnectorConformanceError)) throw error;
    expect(error.report.conformant).toBe(false);
    expect(error.report.cases[0]?.name).toBe('validated timeout name');
    expect(error.report.findings).toEqual([
      {
        code: 'CASE_TIMEOUT',
        case: 'validated timeout name',
        reason: "case 'validated timeout name' timed out after 50 ms",
      },
      {
        code: 'INSTRUMENTATION_REPLACED',
        case: 'validated timeout name',
        reason:
          'globalThis.fetch descriptor differs from the one the harness installed: data property (writable: true, configurable: true)',
      },
      {
        code: 'CASE_TIMEOUT',
        reason:
          "skipped cases skipped after CASE_TIMEOUT in 'validated timeout name'",
      },
    ]);
    expect(name).toHaveBeenCalledTimes(1);
    expect(Object.getOwnPropertyDescriptor(globalThis, 'fetch')).toEqual(saved);
    const later = await sdk
      .assertConnectorConformance(subject, {
        manifest: noEgress,
        cases: [quietCase],
      })
      .catch((error: unknown) => error);
    expect(later).toBeInstanceOf(sdk.ConnectorConformanceError);
    if (!(later instanceof sdk.ConnectorConformanceError)) throw later;
    expect(later.report.findings).toEqual([
      {
        code: 'ISOLATE_POISONED',
        reason:
          "case 'validated timeout name' timed out in this isolate; no further run is accepted",
      },
    ]);
    expect(Object.getOwnPropertyDescriptor(globalThis, 'fetch')).toEqual(saved);
  });

  it('reports INSTRUMENTATION_REPLACED and CASE_TIMEOUT when a redefined trap outlives a timed-out case', async () => {
    // #given
    vi.resetModules();
    const sdk = await import('./index.js');
    const saved = Object.getOwnPropertyDescriptor(globalThis, 'fetch');
    const replacement = vi.fn(async () => new Response());
    // #when
    const run = sdk.assertConnectorConformance(
      (runtime) =>
        sdk.createConnector({
          id: 'vendor.timeout',
          description: 'Replacement timeout fixture',
          permissions: noEgress,
          policies: runtime.policies,
          execute: async () => {
            Object.defineProperty(globalThis, 'fetch', { value: replacement });
            return new Promise(() => {});
          },
        }),
      {
        manifest: noEgress,
        cases: [
          { ...quietCase, timeoutMs: 50 },
          { ...quietCase, name: 'skipped' },
        ],
      },
    );
    const error = await run.catch((error: unknown) => error);
    // #then
    expect(error).toBeInstanceOf(sdk.ConnectorConformanceError);
    if (!(error instanceof sdk.ConnectorConformanceError)) throw error;
    const report = error.report;
    expect(report.conformant).toBe(false);
    expect(report.cases).toHaveLength(1);
    expect(report.cases[0]).toMatchObject({
      proved: 'nothing',
      guardedHosts: [],
      decisionCodes: [],
      transportCalls: 0,
      findings: [
        expect.objectContaining({ code: 'CASE_TIMEOUT' }),
        expect.objectContaining({ code: 'INSTRUMENTATION_REPLACED' }),
      ],
    });
    expect(report.findings).toContainEqual({
      code: 'CASE_TIMEOUT',
      reason: "skipped cases skipped after CASE_TIMEOUT in 'quiet'",
    });
    expect(Object.getOwnPropertyDescriptor(globalThis, 'fetch')).toEqual(saved);
  });

  it('restores instrumentation when a case never settles', async () => {
    // #given
    const saved = globalThis.fetch;
    const execute = vi.fn(async () => new Promise(() => {}));
    // #when
    const report = await rejected(
      assertConnectorConformance(quietFactory(execute), {
        manifest: noEgress,
        cases: [
          { ...quietCase, name: 'never settles', timeoutMs: 50 },
          { ...quietCase, name: 'skipped' },
        ],
      }),
    );
    // #then
    expect(globalThis.fetch).toBe(saved);
    expect(execute).toHaveBeenCalledTimes(1);
    expect(report.cases).toHaveLength(1);
    expect(report.cases[0]?.proved).toBe('nothing');
    expect(report.cases[0]?.guardedHosts).toEqual([]);
    expect(report.cases[0]?.decisionCodes).toEqual([]);
    expect(report.cases[0]?.findings).toEqual([
      expect.objectContaining({ code: 'CASE_TIMEOUT' }),
    ]);
    expect(report.findings).toContainEqual({
      code: 'CASE_TIMEOUT',
      reason: "skipped cases skipped after CASE_TIMEOUT in 'never settles'",
    });
  });

  it('refuses a later run in an isolate where a case timed out', async () => {
    // #given
    const saved = globalThis.fetch;
    const subject = vi.fn(factory());
    // #when
    const report = await rejected(
      assertConnectorConformance(subject, { manifest, cases: [requestCase] }),
    );
    // #then
    expect(report.findings).toEqual([
      expect.objectContaining({
        code: 'ISOLATE_POISONED',
        reason: expect.stringContaining('never settles'),
      }),
    ]);
    expect(report.cases).toEqual([]);
    expect(report.instrumented).toEqual([]);
    expect(subject).not.toHaveBeenCalled();
    expect(globalThis.fetch).toBe(saved);
  });
});
