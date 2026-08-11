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

describe('assertAttwEsmReport', () => {
  it('accepts only the exact known Node10 internal-resolution exception', () => {
    expect(() =>
      assertAttwEsmReport(
        report([
          {
            kind: 'InternalResolutionError',
            resolutionOption: 'node10',
            moduleSpecifier: '#deployment-identity-protocol',
            fileName:
              '/node_modules/@proofoftech/flowsafe/dist/do-runner/deployment-identity.d.ts',
          },
        ]),
        1,
      ),
    ).not.toThrow();
  });

  it('rejects unknown problem kinds even when their resolution is absent', () => {
    expect(() =>
      assertAttwEsmReport(report([{ kind: 'FalseESM' }]), 0),
    ).toThrow(/unexpected ATTW problem/);
  });
});
