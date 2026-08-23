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
  // Node 10 resolution predates subpath `imports`, so every declaration that
  // imports `#deployment-identity-protocol` raises one internal-resolution
  // error under that option. The package publishes esm-only and node10 is
  // already an ignored profile (see IGNORED_PROFILE_PROBLEMS), so these are
  // accepted — but pinned three ways, because an internal import that genuinely
  // does not resolve (a broken relative path in the emitted types) is exactly
  // what this check exists to catch, and it would carry a different specifier.
  //
  // The file list is deliberate, not a count. Adding a module that imports the
  // protocol means adding it here — a decision, since each one is another
  // declaration a Node 10 consumer cannot resolve.
  const PROTOCOL_IMPORTING_DECLARATIONS = [
    '/dist/do-runner/deployment-identity.d.ts',
    '/dist/do-runner/execution-fence.d.ts',
  ];
  for (const problem of internal) {
    const detail = JSON.stringify({
      fileName: problem.fileName,
      moduleSpecifier: problem.moduleSpecifier,
      resolutionOption: problem.resolutionOption,
    });
    assert.equal(
      problem.resolutionOption,
      'node10',
      `unexpected ATTW internal-resolution error: ${detail}`,
    );
    assert.equal(
      problem.moduleSpecifier,
      '#deployment-identity-protocol',
      `unexpected ATTW internal-resolution error: ${detail}`,
    );
    assert.ok(
      PROTOCOL_IMPORTING_DECLARATIONS.some((suffix) =>
        problem.fileName.endsWith(suffix),
      ),
      `unexpected ATTW internal-resolution error: ${detail}`,
    );
  }
  // ATTW exits non-zero when it reports anything the profile does not ignore.
  // Under esm-only the accepted node10 entries above are the only such
  // problems, so a non-zero exit must be explained by them and a zero exit must
  // mean there were none.
  assert.equal(
    status === 0,
    internal.length === 0,
    `ATTW exited ${status}\n${stderr}`,
  );

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
