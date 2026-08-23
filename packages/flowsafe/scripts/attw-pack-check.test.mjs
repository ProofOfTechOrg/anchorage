import { describe, expect, it } from 'vitest';
import { assertAttwEsmReport } from './attw-pack-check.mjs';

function report(problems) {
  return {
    analysis: {
      entrypoints: {
        '.': {
          resolutions: {
            'node16-esm': { visibleProblems: [] },
            bundler: { visibleProblems: [] },
          },
        },
      },
      problems,
    },
  };
}

function internalResolutionError(overrides = {}) {
  return {
    kind: 'InternalResolutionError',
    resolutionOption: 'node10',
    moduleSpecifier: '#deployment-identity-protocol',
    fileName:
      '/node_modules/@proofoftech/flowsafe/dist/do-runner/deployment-identity.d.ts',
    ...overrides,
  };
}

describe('assertAttwEsmReport', () => {
  it('accepts every declaration that imports the protocol under Node10', () => {
    expect(() =>
      assertAttwEsmReport(
        report([
          internalResolutionError(),
          internalResolutionError({
            fileName:
              '/node_modules/@proofoftech/flowsafe/dist/do-runner/execution-fence.d.ts',
          }),
        ]),
        1,
      ),
    ).not.toThrow();
  });

  it('rejects an internal-resolution error that is not the known exception', () => {
    // A genuinely broken relative import in the emitted types — the failure
    // this check exists to catch — differs in specifier, file, or option.
    for (const overrides of [
      { moduleSpecifier: './missing-module.js' },
      { resolutionOption: 'node16-esm' },
      {
        fileName:
          '/node_modules/@proofoftech/flowsafe/dist/host-kit/index.d.ts',
      },
    ]) {
      expect(() =>
        assertAttwEsmReport(report([internalResolutionError(overrides)]), 1),
      ).toThrow(/unexpected ATTW internal-resolution error/);
    }
  });

  it('rejects a zero exit that contradicts the reported problems', () => {
    expect(() =>
      assertAttwEsmReport(report([internalResolutionError()]), 0),
    ).toThrow(/ATTW exited 0/);
    expect(() => assertAttwEsmReport(report([]), 1)).toThrow(/ATTW exited 1/);
  });

  it('rejects unknown problem kinds even when their resolution is absent', () => {
    expect(() =>
      assertAttwEsmReport(report([{ kind: 'FalseESM' }]), 0),
    ).toThrow(/unexpected ATTW problem/);
  });
});
