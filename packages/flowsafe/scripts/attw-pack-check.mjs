import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import {
  closeSync,
  mkdtempSync,
  openSync,
  readFileSync,
  rmSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const IGNORED_PROFILE_PROBLEMS = new Map([
  ['NoResolution', 'node10'],
  ['CJSResolvesToESM', 'node16-cjs'],
]);

export function assertAttwEsmPackage(archive, cwd) {
  const attw = join(resolve(cwd, '..', '..'), 'node_modules', '.bin', 'attw');
  const temporary = mkdtempSync(join(tmpdir(), 'flowsafe-attw-'));
  const output = join(temporary, 'report.json');
  const descriptor = openSync(output, 'w');
  const result = spawnSync(
    attw,
    [
      archive,
      '--profile',
      'esm-only',
      '--format',
      'json',
      '--no-summary',
      '--no-color',
    ],
    {
      cwd,
      encoding: 'utf8',
      stdio: ['ignore', descriptor, 'pipe'],
    },
  );
  closeSync(descriptor);
  if (result.error) throw result.error;
  assert.equal(result.signal, null, `ATTW terminated via ${result.signal}`);

  let report;
  try {
    report = JSON.parse(readFileSync(output, 'utf8'));
  } catch (error) {
    throw new Error(`ATTW returned invalid JSON\n${result.stderr}`, {
      cause: error,
    });
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }

  assertAttwEsmReport(report, result.status, result.stderr);
}

export function assertAttwEsmReport(report, status, stderr = '') {
  const analysis = report.analysis;
  assert.ok(analysis?.entrypoints && Array.isArray(analysis.problems));
  for (const [entrypoint, entry] of Object.entries(analysis.entrypoints)) {
    for (const resolutionKind of ['node16-esm', 'bundler']) {
      assert.deepEqual(
        entry.resolutions?.[resolutionKind]?.visibleProblems,
        [],
        `${entrypoint} has ATTW ${resolutionKind} problems`,
      );
    }
  }

  const internal = analysis.problems.filter(
    (problem) => problem.kind === 'InternalResolutionError',
  );
  assert.ok(internal.length <= 1, 'unexpected ATTW internal-resolution errors');
  if (internal[0]) {
    assert.equal(internal[0].resolutionOption, 'node10');
    assert.equal(internal[0].moduleSpecifier, '#deployment-identity-protocol');
    assert.match(
      internal[0].fileName,
      /\/dist\/do-runner\/deployment-identity\.d\.ts$/,
    );
  }
  assert.equal(status, internal.length, `ATTW exited ${status}\n${stderr}`);

  for (const problem of analysis.problems) {
    if (problem.kind === 'InternalResolutionError') continue;
    assert.ok(
      IGNORED_PROFILE_PROBLEMS.has(problem.kind),
      `unexpected ATTW problem: ${JSON.stringify(problem)}`,
    );
    assert.equal(
      problem.resolutionKind,
      IGNORED_PROFILE_PROBLEMS.get(problem.kind),
      `unexpected ATTW problem: ${JSON.stringify(problem)}`,
    );
  }
}
