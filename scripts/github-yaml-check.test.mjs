import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { afterEach, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { parse } from 'yaml';
import {
  checkGithubYamlFiles,
  runGithubYamlCheck,
} from './github-yaml-check.mjs';
import {
  PROBE_TIMEOUT_MS,
  PUBLISH_PREREQUISITES,
  VISIBILITY_DEADLINE_MS,
} from './publish-ordered.mjs';
import {
  NODE_HANDLER_TYPES,
  PNPM_OPTION_POLICIES,
  UNMODELLED_NODE_TYPES,
} from './shell-command-analysis.mjs';

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const require = createRequire(import.meta.url);

const temporaryDirectories = [];
const realTimeout = spawnSync('which', ['timeout'], {
  encoding: 'utf8',
}).stdout.trim();

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { force: true, recursive: true });
  }
});

function fixture(files) {
  const githubDirectory = scratchDirectory('anchorage-github-yaml-');
  for (const [path, contents] of Object.entries(files)) {
    const absolutePath = join(githubDirectory, path);
    mkdirSync(dirname(absolutePath), { recursive: true });
    writeFileSync(absolutePath, contents);
  }
  return githubDirectory;
}

function scratchDirectory(prefix) {
  const githubDirectory = mkdtempSync(join(tmpdir(), prefix));
  temporaryDirectories.push(githubDirectory);
  return githubDirectory;
}

function runShell(script, env, options = {}) {
  return spawnSync('bash', ['-e', '-c', script], {
    cwd: options.cwd,
    encoding: 'utf8',
    env: { ...process.env, ...env },
    timeout: 5_000,
  });
}

function captureRun(githubDirectory) {
  let stderr = '';
  let stdout = '';
  const exitCode = runGithubYamlCheck(githubDirectory, {
    stderr: { write: (chunk) => (stderr += chunk) },
    stdout: { write: (chunk) => (stdout += chunk) },
  });
  return { exitCode, stderr, stdout };
}

// The assertion is read out of the tracked workflow, not restated here, so the
// case evaluates what ships rather than a copy of it.
function readWorkflow() {
  return parse(
    readFileSync(join(repositoryRoot, '.github/workflows/ci.yml'), 'utf8'),
  );
}

function readVerifyGateJob() {
  const job = readWorkflow().jobs.verify;
  assert.ok(
    job,
    'the `protect main` ruleset requires the status check named `verify`, which is this job',
  );
  const steps = (job.steps ?? []).filter((step) => 'run' in step);
  assert.equal(steps.length, 1, 'the verify gate job has one run step');
  return { job, step: steps[0] };
}

function readVerifyCoreJob() {
  const job = readWorkflow().jobs['verify-core'];
  assert.ok(job, 'the CI workflow contains the `verify-core` job');
  return job;
}

function bashFenceAfter(path, marker) {
  const lines = readFileSync(join(repositoryRoot, path), 'utf8').split('\n');
  const markerIndex = lines.findIndex((line) => line.includes(marker));
  assert.notEqual(markerIndex, -1, `${path} contains '${marker}'`);
  const fenceStart = lines.findIndex(
    (line, index) => index > markerIndex && line === '```bash',
  );
  assert.notEqual(fenceStart, -1, `${path} has a bash fence after '${marker}'`);
  const fenceEnd = lines.indexOf('```', fenceStart + 1);
  assert.notEqual(fenceEnd, -1, `${path} closes its bash fence`);
  return lines.slice(fenceStart + 1, fenceEnd).filter(Boolean);
}

function runVerifyGate(step, needs) {
  const variables = Object.keys(step.env ?? {});
  assert.equal(
    variables.length,
    1,
    'the verify gate step carries the needs context in one env variable',
  );
  return runShell(step.run, { [variables[0]]: JSON.stringify(needs) });
}

function resultListing(needs) {
  return Object.entries(needs).map(([job, { result }]) => `${job}: ${result}`);
}

function readCompatCanaryJob() {
  const job = readWorkflow().jobs['mastra-compat'];
  assert.ok(job, 'the compat canary is the job named `mastra-compat`');
  return job;
}

function canaryStep(id) {
  const step = (readCompatCanaryJob().steps ?? []).find(
    (candidate) => candidate.id === id,
  );
  assert.ok(step, `the canary runs a step with the id \`${id}\``);
  return step;
}

// The lookup retry is shell, and its properties — a retried blip, a permanent
// E404, a refused body, npm's stderr staying out of the payload — are shell
// semantics only a run settles. Both the function and the assignment that
// consumes it come out of the tracked step, because the assignment is where
// the function's contract is observed: errexit does not reach inside `$( )`,
// so a lookup that reports success with nothing to say is visible only there.
function newestVersionScript() {
  const { run } = canaryStep('mastra_versions');
  const start = run.indexOf('newest_version() {');
  const end = run.indexOf('\n}\n', start);
  const caller = run.match(/^CORE_VERSION=.+$/mu);
  assert.ok(
    start >= 0 && end > start,
    'the bump step looks up versions through a `newest_version` function',
  );
  assert.ok(caller, 'the bump step captures a lookup in CORE_VERSION');
  return [
    run.slice(start, end + 3),
    `sleep() { printf 'sleep\\n' >> "$CALL_LOG"; }`,
    caller[0],
    `printf '%s\\n' "$CORE_VERSION"`,
  ].join('\n');
}

/** An `npm` that records its argv, over the body a case gives it. */
function npmStub(...body) {
  return [
    '#!/usr/bin/env bash',
    `printf 'npm %s\\n' "$*" >> "$CALL_LOG"`,
    ...body,
  ].join('\n');
}

const NPM_STUBS = {
  newest: npmStub(`printf '["1.66.0","1.67.0"]\\n'`),
  blipThenNewest: npmStub(
    `if [ "$(grep -c '^npm ' "$CALL_LOG")" -lt 2 ]; then`,
    `  printf 'npm error code E500\\n' >&2`,
    '  exit 1',
    'fi',
    `printf '"1.67.0"\\n'`,
  ),
  unreachable: npmStub(`printf 'npm error code E500\\n' >&2`, 'exit 1'),
  missing: npmStub(
    `printf '{"error":{"code":"E404","summary":"no match found"}}\\n'`,
    `printf 'npm error code E404\\n' >&2`,
    'exit 1',
  ),
  unparsable: npmStub(`printf 'not a JSON body\\n'`),
  empty: npmStub(`printf '""\\n'`),
  newestWithWarning: npmStub(
    `printf 'npm warn Unknown env config "x"\\n' >&2`,
    `printf '["1.67.0"]\\n'`,
  ),
  e404ThenNewest: npmStub(
    `if [ "$(grep -c '^npm ' "$CALL_LOG")" -lt 2 ]; then`,
    `  printf 'npm error code E404\\n'`,
    '  while :; do sleep 1; done',
    'fi',
    `printf '"1.67.0"\\n'`,
  ),
  repeatedKilledE404: npmStub(
    `printf 'npm error code E404\\n'`,
    'while :; do sleep 1; done',
  ),
};

function runNewestVersion(stub) {
  const directory = scratchDirectory('anchorage-npm-stub-');
  const callLog = join(directory, 'calls');
  writeFileSync(callLog, '');
  writeFileSync(join(directory, 'npm'), stub, { mode: 0o755 });
  writeFileSync(
    join(directory, 'timeout'),
    `#!/usr/bin/env bash
set -e
printf 'timeout %s\\n' "$*" >> "$CALL_LOG"
test "$1" = '--signal=KILL'
test "$2" = '90s'
shift 2
exec "$REAL_TIMEOUT" --signal=KILL 0.1s "$@"
`,
    { mode: 0o755 },
  );

  const run = runShell(newestVersionScript(), {
    CALL_LOG: callLog,
    PATH: `${directory}:${process.env.PATH}`,
    REAL_TIMEOUT: realTimeout,
  });

  const calls = readFileSync(callLog, 'utf8').split('\n').filter(Boolean);
  return {
    error: run.error,
    status: run.status,
    stderr: run.stderr,
    stdout: run.stdout,
    lookups: calls.filter((line) => line.startsWith('npm ')),
    sleeps: calls.filter((line) => line === 'sleep').length,
    timeouts: calls.filter((line) => line.startsWith('timeout ')),
  };
}

const STEP_OUTCOME = /\$\{\{\s*steps\.(\w+)\.outcome\s*\}\}/gu;

function outcomeReferences(step) {
  return [...step.run.matchAll(STEP_OUTCOME)].map(([, id]) => id);
}

/** The outcome step's own body, with the step contexts a case supplies. */
function runProbeOutcome(step, outcomes) {
  for (const id of outcomeReferences(step)) {
    assert.ok(id in outcomes, `this case supplies no outcome for \`${id}\``);
  }
  const script = step.run.replaceAll(
    STEP_OUTCOME,
    (_match, id) => outcomes[id],
  );
  assert.ok(
    !script.includes('${{'),
    `the outcome step reads a context this case cannot supply: ${step.run}`,
  );

  const directory = scratchDirectory('anchorage-step-summary-');
  const summaryFile = join(directory, 'summary');
  const run = runShell(script, { GITHUB_STEP_SUMMARY: summaryFile });
  return { run, summary: readFileSync(summaryFile, 'utf8') };
}

function runMastraVersionAssertion({
  baselineCore,
  baselineD1,
  expectedCore,
  expectedD1,
  installedCore,
  installedD1,
}) {
  const step = (readCompatCanaryJob().steps ?? []).find(
    ({ name }) => name === 'Assert the newest 1.x packages are installed',
  );
  assert.ok(step, 'the canary asserts its requested package versions');
  const directory = scratchDirectory('anchorage-mastra-versions-');
  const summaryFile = join(directory, 'summary');
  writeFileSync(summaryFile, '');
  writeFileSync(
    join(directory, 'pnpm'),
    `#!/usr/bin/env bash
case "$*" in
  *breakwater*) printf '%s\\n' "$INSTALLED_CORE" ;;
  *flowsafe*) printf '%s\\n' "$INSTALLED_D1" ;;
  *) exit 2 ;;
esac
`,
    { mode: 0o755 },
  );
  const run = runShell(
    step.run,
    {
      BASELINE_CORE: baselineCore,
      BASELINE_D1: baselineD1,
      EXPECTED_CORE: expectedCore,
      EXPECTED_D1: expectedD1,
      GITHUB_STEP_SUMMARY: summaryFile,
      INSTALLED_CORE: installedCore,
      INSTALLED_D1: installedD1,
      PATH: `${directory}:${process.env.PATH}`,
    },
    { cwd: repositoryRoot },
  );
  return { run, summary: readFileSync(summaryFile, 'utf8') };
}

test('counts a valid YAML mapping', () => {
  const result = checkGithubYamlFiles(
    fixture({ 'workflow.yml': 'name: CI\n' }),
  );

  assert.equal(result.filesChecked, 1);
  assert.deepEqual(result.errors, []);
});

test('reports the plain-scalar colon shape with its file and line', () => {
  const githubDirectory = fixture({
    'workflows/ci.yml': `jobs:
  verify:
    steps:
      - run: echo "x": "y"
`,
  });
  const result = checkGithubYamlFiles(githubDirectory);

  assert.equal(result.errors.length, 1);
  assert.equal(result.errors[0].code, 'BLOCK_AS_IMPLICIT_KEY');
  assert.equal(
    result.errors[0].file,
    `${basename(githubDirectory)}/workflows/ci.yml`,
  );
  assert.equal(result.errors[0].line, 4);
});

test('reports every parser error in one file', () => {
  const result = checkGithubYamlFiles(
    fixture({
      'workflows/ci.yml': `first: echo "x": "y"
second: echo "a": "b"
`,
    }),
  );

  assert.equal(result.errors.length, 2);
  assert.deepEqual(
    result.errors.map(({ code, line }) => ({ code, line })),
    [
      { code: 'BLOCK_AS_IMPLICIT_KEY', line: 1 },
      { code: 'BLOCK_AS_IMPLICIT_KEY', line: 2 },
    ],
  );
});

test('reports unresolved aliases without skipping later broken files', () => {
  let result;
  let githubDirectory;
  assert.doesNotThrow(() => {
    githubDirectory = fixture({
      'aliases.yml': 'jobs: *nope\n',
      'workflows/broken.yml': 'run: echo "x": "y"\n',
    });
    result = checkGithubYamlFiles(githubDirectory);
  });

  const directory = basename(githubDirectory);
  assert.deepEqual(
    result.errors.map(({ file, code }) => ({ file, code })),
    [
      { file: `${directory}/aliases.yml`, code: 'UNRESOLVED_ALIAS' },
      {
        file: `${directory}/workflows/broken.yml`,
        code: 'BLOCK_AS_IMPLICIT_KEY',
      },
    ],
  );
});

test('rejects multiple YAML documents', () => {
  const result = checkGithubYamlFiles(
    fixture({ 'multiple.yml': 'one: 1\n---\ntwo: 2\n' }),
  );

  assert.equal(result.errors.length, 1);
  assert.equal(result.errors[0].code, 'MULTIPLE_DOCS');
});

test('rejects empty and comment-only documents as non-mappings', () => {
  const result = checkGithubYamlFiles(
    fixture({ 'empty.yml': '', 'only-comment.yml': '# no data\n' }),
  );

  assert.equal(result.errors.length, 2);
  assert.ok(
    result.errors.every((error) => error.message === 'not a YAML mapping'),
  );
});

test('rejects tagged maps and binary scalars as non-mappings', () => {
  const result = checkGithubYamlFiles(
    fixture({
      'binary.yml': '!!binary "SGVsbG8="\n',
      'omap.yml': '!!omap\n- a: 1\n',
    }),
  );

  assert.equal(result.errors.length, 2);
  assert.ok(
    result.errors.every((error) => error.message === 'not a YAML mapping'),
  );
});

test('rejects forbidden characters and continues checking later files', () => {
  const githubDirectory = fixture({
    'control.yml': 'name: valid\nvalue: "bell \u0007"\n',
    'workflows/broken.yml': 'run: echo "x": "y"\n',
  });
  const result = checkGithubYamlFiles(githubDirectory);
  const run = captureRun(githubDirectory);

  assert.equal(run.exitCode, 1);
  assert.deepEqual(
    result.errors.map(({ file, line, column, code, message }) => ({
      file,
      line,
      column,
      code,
      message,
    })),
    [
      {
        file: `${basename(githubDirectory)}/control.yml`,
        line: 2,
        column: 14,
        code: 'FORBIDDEN_CHARACTER',
        message: 'forbidden character U+0007 is not allowed',
      },
      {
        file: `${basename(githubDirectory)}/workflows/broken.yml`,
        line: 1,
        column: 6,
        code: 'BLOCK_AS_IMPLICIT_KEY',
        message:
          'Nested mappings are not allowed in compact mappings at line 1, column 6: run: echo "x": "y" ^',
      },
    ],
  );
});

test('rejects malformed UTF-8 and continues checking later files', () => {
  const githubDirectory = fixture({
    'invalid.yml': Buffer.from([
      0x6e, 0x61, 0x6d, 0x65, 0x3a, 0x20, 0xc3, 0x28, 0x0a,
    ]),
    'workflows/broken.yml': 'run: echo "x": "y"\n',
  });
  const result = checkGithubYamlFiles(githubDirectory);
  const run = captureRun(githubDirectory);

  assert.equal(run.exitCode, 1);
  assert.deepEqual(
    result.errors.map(({ file, code }) => ({ file, code })),
    [
      {
        file: `${basename(githubDirectory)}/invalid.yml`,
        code: 'INVALID_UTF8',
      },
      {
        file: `${basename(githubDirectory)}/workflows/broken.yml`,
        code: 'BLOCK_AS_IMPLICIT_KEY',
      },
    ],
  );
});

test('rejects YAML non-characters', () => {
  const githubDirectory = fixture({
    'non-character.yml': 'name: "bad \uFFFE and \uFFFF"\n',
  });
  const result = checkGithubYamlFiles(githubDirectory);

  assert.deepEqual(result.errors, [
    {
      file: `${basename(githubDirectory)}/non-character.yml`,
      line: 1,
      column: 12,
      code: 'FORBIDDEN_CHARACTER',
      message: 'forbidden character U+FFFE is not allowed',
    },
  ]);
});

test('rejects symlinks without parsing targets outside the GitHub directory', () => {
  const githubDirectory = fixture({ 'workflows/ci.yml': 'name: CI\n' });
  const targetDirectory = mkdtempSync(join(tmpdir(), 'anchorage-yaml-target-'));
  temporaryDirectories.push(targetDirectory);
  const target = join(targetDirectory, 'outside.yml');
  writeFileSync(target, 'run: echo "x": "y"\n');
  symlinkSync(target, join(githubDirectory, 'workflows/linked.yml'));

  const result = checkGithubYamlFiles(githubDirectory);
  const run = captureRun(githubDirectory);

  assert.equal(run.exitCode, 1);
  assert.deepEqual(
    result.errors.map(({ file, code }) => ({ file, code })),
    [
      {
        file: `${basename(githubDirectory)}/workflows/linked.yml`,
        code: 'SYMLINK',
      },
    ],
  );
  assert.ok(
    result.errors.every(({ code }) => code !== 'BLOCK_AS_IMPLICIT_KEY'),
  );
});

test('rejects a symlinked GitHub root without reading its target', () => {
  const targetDirectory = mkdtempSync(join(tmpdir(), 'anchorage-yaml-target-'));
  temporaryDirectories.push(targetDirectory);
  writeFileSync(join(targetDirectory, 'broken.yml'), 'run: echo "x": "y"\n');
  const parent = mkdtempSync(join(tmpdir(), 'anchorage-github-yaml-'));
  temporaryDirectories.push(parent);
  const githubDirectory = join(parent, '.github');
  symlinkSync(targetDirectory, githubDirectory);

  const result = checkGithubYamlFiles(githubDirectory);
  const run = captureRun(githubDirectory);

  assert.equal(run.exitCode, 1);
  assert.deepEqual(result, {
    filesChecked: 0,
    errors: [
      {
        file: '.github',
        line: 1,
        column: 1,
        code: 'SYMLINK',
        message: 'symbolic links are not validated; commit the file directly',
      },
    ],
    warnings: [],
  });
});

test('fails closed when the GitHub YAML directory is a plain file', () => {
  const parent = fixture({});
  const githubDirectory = join(parent, 'not-a-directory');
  writeFileSync(githubDirectory, 'name: CI\n');

  const result = checkGithubYamlFiles(githubDirectory);
  const run = captureRun(githubDirectory);

  assert.equal(run.exitCode, 1);
  assert.deepEqual(result, {
    filesChecked: 0,
    errors: [
      {
        file: 'not-a-directory',
        line: 1,
        column: 1,
        code: 'NOT_A_DIRECTORY',
        message: 'GitHub YAML path is not a directory: not-a-directory',
      },
    ],
    warnings: [],
  });
});

test('includes nested files with both YAML extensions', () => {
  const result = checkGithubYamlFiles(
    fixture({
      'ISSUE_TEMPLATE/bug.yaml': 'name: Bug\n',
      'workflows/ci.yml': 'name: CI\n',
    }),
  );

  assert.equal(result.filesChecked, 2);
  assert.deepEqual(result.errors, []);
});

test('fails closed when no YAML files exist', () => {
  const githubDirectory = fixture({ 'README.md': '# GitHub\n' });
  const result = checkGithubYamlFiles(githubDirectory);

  assert.equal(result.filesChecked, 0);
  assert.equal(result.errors.length, 1);
  assert.equal(result.errors[0].code, 'NO_YAML_FILES');
  assert.equal(result.errors[0].file, basename(githubDirectory));
});

test('fails closed when the GitHub YAML directory is missing', () => {
  const parent = fixture({ 'workflow.yml': 'name: CI\n' });
  const githubDirectory = join(parent, 'missing-github');
  const result = checkGithubYamlFiles(githubDirectory);

  assert.deepEqual(result, {
    filesChecked: 0,
    errors: [
      {
        file: 'missing-github',
        line: 1,
        column: 1,
        code: 'MISSING_DIRECTORY',
        message: 'GitHub YAML directory does not exist: missing-github',
      },
    ],
    warnings: [],
  });
  assert.equal(captureRun(githubDirectory).exitCode, 1);
});

test('prints parser warnings without failing the run', () => {
  const githubDirectory = fixture({
    'directive.yml': '%FOO bar\n---\na: 1\n',
  });
  const run = captureRun(githubDirectory);

  assert.equal(run.exitCode, 0);
  assert.match(
    run.stderr,
    new RegExp(
      `^WARN ${basename(githubDirectory)}/directive\\.yml:1:1 BAD_DIRECTIVE:`,
    ),
  );
  assert.equal(run.stdout, 'GitHub YAML check passed (1 files).\n');
});

test('flags a pnpm invocation before an install, in whichever spelling installs', () => {
  const githubDirectory = fixture({
    'workflows/ci.yml': `jobs:
  uninstalled:
    steps:
      - name: Read the installed version
        run: |
          VERSION=$(pnpm --filter pkg exec node -p 1)
      - name: Build
        run: pnpm -r build
  installed:
    steps:
      - run: pnpm install --frozen-lockfile
      - name: Build
        run: pnpm -r build
  short-install:
    steps:
      - run: pnpm i --frozen-lockfile
      - name: Build
        run: pnpm -r build
  flags-before-the-subcommand:
    steps:
      - name: Show pnpm
        run: pnpm --version
      - run: pnpm -r install
      - name: Build
        run: pnpm --frozen-lockfile install
  action-setup-install:
    steps:
      - uses: pnpm/action-setup@v4
        with:
          run_install: true
      - name: Build
        run: pnpm -r build
  action-setup-without-install:
    steps:
      - uses: pnpm/action-setup@v4
      - name: Build
        run: pnpm -r build
`,
  });
  const result = checkGithubYamlFiles(githubDirectory);

  assert.deepEqual(
    result.errors.map(({ file, code, message }) => ({ file, code, message })),
    [
      {
        file: `${basename(githubDirectory)}/workflows/ci.yml`,
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `uninstalled` invokes pnpm in `Read the installed version` with no earlier install step: pnpm --filter pkg exec node -p 1',
      },
      {
        file: `${basename(githubDirectory)}/workflows/ci.yml`,
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `action-setup-without-install` invokes pnpm in `Build` with no earlier install step: pnpm -r build',
      },
    ],
  );
});

function pnpmDiagnostics(workflow) {
  return checkGithubYamlFiles(
    fixture({ 'workflows/ci.yml': workflow }),
  ).errors.filter(({ code }) =>
    ['MISSING_PNPM_INSTALL', 'PNPM_ANALYSIS_LIMIT'].includes(code),
  );
}

function pnpmDiagnosticSummary(workflow) {
  return pnpmDiagnostics(workflow).map(({ code, message }) => ({
    code,
    message,
  }));
}

function missingPnpm(job, step, command) {
  return {
    code: 'MISSING_PNPM_INSTALL',
    message: `job \`${job}\` invokes pnpm in \`${step}\` with no earlier install step: ${command}`,
  };
}

function limitedPnpm(job, step, command, reason) {
  return {
    code: 'PNPM_ANALYSIS_LIMIT',
    message: `job \`${job}\` step \`${step}\` cannot be shown to run pnpm only after an install (${reason ?? 'install is not guaranteed'}): ${command}`,
  };
}

function githubExpression(body) {
  return ['$', '{{ ', body, ' }}'].join('');
}

// hasPnpmToken keeps conservative limit analysis inside fixtures that model a
// non-literal pnpm spelling while preserving the reported command text.
function withPnpmToken(source) {
  return `${source}; : pnpm`;
}

function indentYamlBlock(source, spaces = 10) {
  const prefix = ' '.repeat(spaces);
  return source
    .split('\n')
    .map((line) => `${prefix}${line}`)
    .join('\n');
}

test('the pnpm analysis reads command structure without treating data as commands', () => {
  const diagnostics = pnpmDiagnostics(`jobs:
  data:
    steps:
      - run: |
          printf '%s' 'pnpm run quoted'
          # pnpm run commented
          cat <<'TEXT'
          pnpm run quoted-heredoc
          TEXT
          pnpm install
          pnpm run build
  expanded-heredoc:
    steps:
      - name: heredoc expansion
        run: |
          cat <<TEXT
          $(pnpm run generated)
          TEXT
          pnpm install
  nested-substitution:
    steps:
      - name: nested expansion
        run: echo "$(printf '%s' "$(pnpm run nested)")"
  backtick-substitution:
    steps:
      - name: backtick expansion
        run: echo \`pnpm run backtick\`
  unicode-prefix:
    steps:
      - name: unicode command
        run: |
          printf 'café'
          pnpm run build
`);

  assert.deepEqual(
    diagnostics.map(({ code, message }) => ({ code, message })),
    [
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `expanded-heredoc` invokes pnpm in `heredoc expansion` with no earlier install step: pnpm run generated',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `nested-substitution` invokes pnpm in `nested expansion` with no earlier install step: pnpm run nested',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `backtick-substitution` invokes pnpm in `backtick expansion` with no earlier install step: pnpm run backtick',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `unicode-prefix` invokes pnpm in `unicode command` with no earlier install step: pnpm run build',
      },
    ],
  );
});

test('the pnpm analysis exports installation facts from successful paths', () => {
  const diagnostics = pnpmDiagnostics(`jobs:
  both-branches:
    steps:
      - run: |
          if test -n "$VALUE"; then pnpm install; else pnpm i; fi
          pnpm run build
  one-branch:
    steps:
      - name: conditional install
        run: |
          if test -n "$VALUE"; then pnpm install; fi
          pnpm run build
  loop:
    steps:
      - name: loop install
        run: |
          while test -n "$VALUE"; do pnpm install; done
          pnpm run build
  braces:
    steps:
      - run: |
          { pnpm install; }
          pnpm run build
  short-circuit:
    steps:
      - name: conditional chain
        run: |
          test -n "$VALUE" && pnpm install || true
          pnpm run build
  matching-condition:
    steps:
      - if: \${{ github.event_name == 'push' }}
        run: pnpm install
      - if: \${{ github.event_name == 'push' }}
        run: pnpm run build
  changing-condition:
    steps:
      - if: \${{ steps.setup.outcome == 'success' }}
        run: pnpm install
      - name: uncertain consumer
        if: \${{ steps.setup.outcome == 'success' }}
        run: pnpm run build
  tolerated-install:
    steps:
      - continue-on-error: true
        run: pnpm install
      - name: build after tolerated failure
        run: pnpm run build
`);

  assert.deepEqual(
    diagnostics.map(({ code, message }) => ({ code, message })),
    [
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `one-branch` invokes pnpm in `conditional install` with no earlier install step: pnpm run build',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `loop` step `loop install` cannot be shown to run pnpm only after an install (install is not guaranteed): pnpm install',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `short-circuit` step `conditional chain` cannot be shown to run pnpm only after an install (install is not guaranteed): pnpm install',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `changing-condition` step `step 1` cannot be shown to run pnpm only after an install (step condition is not stable): $' +
          "{{ steps.setup.outcome == 'success' }}",
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `tolerated-install` invokes pnpm in `build after tolerated failure` with no earlier install step: pnpm run build',
      },
    ],
  );
});

test('the pnpm command policy distinguishes consumers from workspace installs', () => {
  const diagnostics = pnpmDiagnostics(`jobs:
  filter-operand:
    steps:
      - name: filter operand
        run: pnpm --filter install run build
  quoted-equals-operand:
    steps:
      - name: quoted equals operand
        run: pnpm --filter="install" run build
  local-add:
    steps:
      - run: pnpm add package
      - run: pnpm run build
  global-add:
    steps:
      - run: pnpm add --global package
      - name: build after global add
        run: pnpm run build
  other-directory:
    steps:
      - run: pnpm --dir packages/example install
      - name: build after other directory
        run: pnpm run build
  lockfile-only:
    steps:
      - run: pnpm install --lockfile-only
      - name: build after lockfile update
        run: pnpm run build
  independent:
    steps:
      - run: pnpm dlx tool && pnpm create package
  wrappers:
    steps:
      - run: env CI=1 pnpm install
      - run: time pnpm run build
  repeated-install:
    steps:
      - run: xargs -r pnpm install
      - name: build after xargs
        run: pnpm run build
  repeated-consumer:
    steps:
      - name: xargs consumer
        run: xargs -n 1 pnpm run build
  unknown-option:
    steps:
      - name: unknown option
        run: pnpm --future-option install run build
`);

  assert.deepEqual(
    diagnostics.map(({ code, message }) => ({ code, message })),
    [
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `filter-operand` invokes pnpm in `filter operand` with no earlier install step: pnpm --filter install run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `quoted-equals-operand` invokes pnpm in `quoted equals operand` with no earlier install step: pnpm --filter="install" run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `global-add` invokes pnpm in `build after global add` with no earlier install step: pnpm run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `other-directory` invokes pnpm in `build after other directory` with no earlier install step: pnpm run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `lockfile-only` invokes pnpm in `build after lockfile update` with no earlier install step: pnpm run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `repeated-install` invokes pnpm in `build after xargs` with no earlier install step: pnpm run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `repeated-consumer` invokes pnpm in `xargs consumer` with no earlier install step: xargs -n 1 pnpm run build',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `unknown-option` step `unknown option` cannot be shown to run pnpm only after an install (unmodelled pnpm option --future-option): pnpm --future-option install run build',
      },
    ],
  );
});

test('install qualifiers have the same meaning on both sides of the pnpm subcommand', () => {
  const commands = [
    'pnpm --global add package',
    'pnpm add --global package',
    'pnpm -g add package',
    'pnpm add -g package',
    'pnpm --lockfile-only install',
    'pnpm install --lockfile-only',
    'pnpm --dir packages/example install',
    'pnpm --dir=packages/example install',
    'pnpm install --dir packages/example',
    'pnpm install --dir=packages/example',
    'pnpm -C packages/example install',
    'pnpm -C=packages/example install',
    'pnpm install -C packages/example',
    'pnpm install -C=packages/example',
    'pnpm --prefix packages/example install',
    'pnpm --prefix=packages/example install',
    'pnpm install --prefix packages/example',
    'pnpm install --prefix=packages/example',
  ];

  for (const [index, command] of commands.entries()) {
    const diagnostics = pnpmDiagnostics(`jobs:
  option-${index}:
    steps:
      - run: ${command}
      - name: workspace consumer
        run: pnpm run build
`);
    assert.deepEqual(
      diagnostics.map(({ code, message }) => ({ code, message })),
      [
        {
          code: 'MISSING_PNPM_INSTALL',
          message: `job \`option-${index}\` invokes pnpm in \`workspace consumer\` with no earlier install step: pnpm run build`,
        },
      ],
      command,
    );
  }
});

test('an unresolved install argv reports the install instead of a later consumer', () => {
  const diagnostics = pnpmDiagnostics(`jobs:
  unknown-after-subcommand:
    steps:
      - name: unknown install option
        run: pnpm install --future-option value
      - run: pnpm run build
  expanded-install-argument:
    steps:
      - name: expanded install argument
        run: pnpm install "$OPTIONS"
      - run: pnpm run build
`);

  assert.deepEqual(
    diagnostics.map(({ code, message }) => ({ code, message })),
    [
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `unknown-after-subcommand` step `unknown install option` cannot be shown to run pnpm only after an install (unmodelled pnpm option --future-option): pnpm install --future-option value',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `expanded-install-argument` step `expanded install argument` cannot be shown to run pnpm only after an install (non-literal pnpm argument): pnpm install "$OPTIONS"',
      },
    ],
  );
});

test('elif, case, and C-style loop installation facts follow their execution paths', () => {
  const diagnostics = pnpmDiagnostics(`jobs:
  elif-without-else:
    steps:
      - name: elif without else
        run: |
          if test -n "$A"; then pnpm install; elif test -n "$B"; then pnpm i; fi
          pnpm run build
  elif-with-else:
    steps:
      - name: elif with else
        run: |
          if test -n "$A"; then true; elif test -n "$B"; then pnpm install; else pnpm i; fi
          pnpm run build
  all-if-arms:
    steps:
      - run: |
          if test -n "$A"; then pnpm install; elif test -n "$B"; then pnpm i; else pnpm add package; fi
          pnpm run build
  case-without-wildcard:
    steps:
      - name: case without wildcard
        run: |
          case "$RUNNER_OS" in Linux) pnpm install ;; Darwin) pnpm i ;; esac
          pnpm run build
  case-with-wildcard:
    steps:
      - run: |
          case "$RUNNER_OS" in Linux) pnpm install ;; *) pnpm i ;; esac
          pnpm run build
  c-style-loop:
    steps:
      - name: C-style loop
        run: |
          for ((index = 0; index < COUNT; index++)); do pnpm install; done
          pnpm run build
`);

  assert.deepEqual(
    diagnostics.map(({ code, message }) => ({ code, message })),
    [
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `elif-without-else` invokes pnpm in `elif without else` with no earlier install step: pnpm run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `elif-with-else` invokes pnpm in `elif with else` with no earlier install step: pnpm run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `case-without-wildcard` invokes pnpm in `case without wildcard` with no earlier install step: pnpm run build',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `c-style-loop` step `C-style loop` cannot be shown to run pnpm only after an install (install is not guaranteed): pnpm install',
      },
    ],
  );
});

test('non-establishing command contexts distinguish installs from consumers', () => {
  const diagnostics = pnpmDiagnostics(`jobs:
  piped-install:
    steps:
      - name: piped install
        run: pnpm install | tee install.log
      - run: pnpm run build
  negated-install:
    steps:
      - name: negated install
        run: '! pnpm install'
      - run: pnpm run build
  piped-consumer:
    steps:
      - name: piped consumer
        run: printf ready | pnpm run build
  negated-consumer:
    steps:
      - name: negated consumer
        run: '! pnpm run build'
  process-substitution-install:
    steps:
      - name: process substitution install
        run: cat < <(pnpm install)
  unnamed-consumer:
    steps:
      - run: |
          printf ready
          pnpm run build
`);

  assert.deepEqual(
    diagnostics.map(({ code, message }) => ({ code, message })),
    [
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `piped-install` step `piped install` cannot be shown to run pnpm only after an install (install is not guaranteed): pnpm install',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `negated-install` step `negated install` cannot be shown to run pnpm only after an install (install is not guaranteed): pnpm install',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `piped-consumer` invokes pnpm in `piped consumer` with no earlier install step: pnpm run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `negated-consumer` invokes pnpm in `negated consumer` with no earlier install step: pnpm run build',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `process-substitution-install` step `process substitution install` cannot be shown to run pnpm only after an install (install is not guaranteed): pnpm install',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `unnamed-consumer` invokes pnpm in `step 1` with no earlier install step: pnpm run build',
      },
    ],
  );
});

test('transparent wrappers expose pnpm consumers while unknown runners limit', () => {
  const wrappers = [
    'command',
    'builtin',
    'exec',
    'sudo',
    'nohup',
    'nice',
    'timeout 10',
    'stdbuf -oL',
    'npx',
    'corepack',
  ];
  const jobs = wrappers
    .map(
      (wrapper, index) => `  wrapper-${index}:
    steps:
      - name: ${wrapper} wrapper
        run: ${wrapper} pnpm run build`,
    )
    .join('\n');
  const diagnostics = pnpmDiagnostics(`jobs:
${jobs}
  lookups:
    steps:
      - run: command -v pnpm
      - run: command -V pnpm
      - run: which pnpm
      - run: type pnpm
      - run: printf '%s' 'pnpm'
`);

  assert.deepEqual(
    diagnostics.map(({ code, message }) => ({ code, message })),
    wrappers.map((wrapper, index) =>
      index < 8
        ? {
            code: 'MISSING_PNPM_INSTALL',
            message: `job \`wrapper-${index}\` invokes pnpm in \`${wrapper} wrapper\` with no earlier install step: ${wrapper} pnpm run build`,
          }
        : {
            code: 'PNPM_ANALYSIS_LIMIT',
            message: `job \`wrapper-${index}\` step \`${wrapper} wrapper\` cannot be shown to run pnpm only after an install (unmodelled ${wrapper} invocation): ${wrapper} pnpm run build`,
          },
    ),
  );
});

test('executable-position syntax is classified by its reduced command word', () => {
  const rows = [
    ['literal', 'pnpm run build', 'missing', 'pnpm run build'],
    ['quoted', '"pnpm" run build', 'missing', '"pnpm" run build'],
    [
      'concatenated',
      withPnpmToken("p'n'pm run build"),
      'missing',
      "p'n'pm run build",
    ],
    [
      'ansi-c',
      "$'pnpm' run build",
      'limit',
      "$'pnpm' run build",
      'non-literal executable',
    ],
    [
      'expansion',
      withPnpmToken('$PM run build'),
      'limit',
      '$PM run build',
      'non-literal executable',
    ],
    [
      'command-substitution',
      '$(which pnpm) run build',
      'limit',
      '$(which pnpm) run build',
      'non-literal executable',
    ],
    [
      'github-expression',
      withPnpmToken(`${githubExpression('matrix.pm')} run build`),
      'limit',
      `${githubExpression('matrix.pm')} run build`,
      'non-literal executable',
    ],
    [
      'backslash',
      withPnpmToken('p\\npm run build'),
      'limit',
      'p\\npm run build',
      'non-literal executable',
    ],
    [
      'line-continuation',
      withPnpmToken('pn\\\npm run build'),
      'limit',
      'pn\\\npm run build',
      'non-literal executable',
    ],
  ];

  for (const [job, command, kind, reportedCommand, reason] of rows) {
    const expected =
      kind === 'missing'
        ? missingPnpm(job, 'probe', reportedCommand)
        : limitedPnpm(job, 'probe', reportedCommand, reason);
    assert.deepEqual(
      pnpmDiagnosticSummary(`jobs:
  ${job}:
    steps:
      - name: probe
        run: ${JSON.stringify(command)}
`),
      [expected],
      job,
    );
  }
});

test('transparent wrappers classify wrapped consumers and installs', () => {
  const wrappers = [
    ['env', 'env CI=1'],
    ['time', 'time'],
    ['command', 'command'],
    ['builtin', 'builtin'],
    ['exec', 'exec'],
    ['sudo', 'sudo'],
    ['nohup', 'nohup'],
    ['nice', 'nice'],
    ['timeout', 'timeout 10'],
    ['stdbuf', 'stdbuf -oL'],
  ];
  for (const [job, wrapper] of wrappers) {
    assert.deepEqual(
      pnpmDiagnosticSummary(`jobs:
  ${job}-consumer:
    steps:
      - name: wrapped consumer
        run: ${wrapper} pnpm run build
  ${job}-install:
    steps:
      - run: ${wrapper} pnpm install
      - run: pnpm run build
`),
      [
        missingPnpm(
          `${job}-consumer`,
          'wrapped consumer',
          `${wrapper} pnpm run build`,
        ),
      ],
      job,
    );
  }

  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
  xargs-consumer:
    steps:
      - name: repeated consumer
        run: xargs -n 1 pnpm run build
  xargs-install:
    steps:
      - run: xargs -n 1 pnpm install
      - name: after repeated install
        run: pnpm run build
  unknown-wrapper-option:
    steps:
      - name: env chdir
        run: env -C elsewhere pnpm install
  clustered-wrapper-option:
    steps:
      - name: background sudo
        run: sudo -nb pnpm install
`),
    [
      missingPnpm(
        'xargs-consumer',
        'repeated consumer',
        'xargs -n 1 pnpm run build',
      ),
      missingPnpm('xargs-install', 'after repeated install', 'pnpm run build'),
      limitedPnpm(
        'unknown-wrapper-option',
        'env chdir',
        'env -C elsewhere pnpm install',
        'unmodelled wrapper option -C',
      ),
      limitedPnpm(
        'clustered-wrapper-option',
        'background sudo',
        'sudo -nb pnpm install',
        'unmodelled wrapper option -b',
      ),
    ],
  );
});

test('code-bearing shells limit while script files and data commands stay opaque', () => {
  const diagnostics = pnpmDiagnosticSummary(`jobs:
  bash-c:
    steps:
      - name: bash command string
        run: bash -c 'pnpm run build'
  bash-s:
    steps:
      - name: bash stdin option
        run: |
${indentYamlBlock(withPnpmToken('bash -s'))}
  here-string:
    steps:
      - name: bash here string
        run: bash <<< 'pnpm run build'
  heredoc:
    steps:
      - name: bash heredoc
        run: |
          bash <<'SCRIPT'
          pnpm run build
          SCRIPT
  pipe:
    steps:
      - name: pipe to shell
        run: printf '%s' 'pnpm run build' | sh
  script-file:
    steps:
      - run: bash script.sh
      - name: later consumer
        run: pnpm run build
  neutral-commands:
    steps:
      - run: |
          command -v "pnpm"
          command -V pnpm
          which pnpm
          type pnpm
          hash pnpm
          echo pnpm
          printf '%s' pnpm
          : pnpm
          true pnpm
          false pnpm
          test pnpm
          [ pnpm ]
          [[ pnpm ]]
          corepack enable pnpm
          corepack prepare pnpm
          corepack use pnpm
          corepack install pnpm
          npm install pnpm
  unknown-runners:
    steps:
      - name: npx runner
        run: npx pnpm run build
  corepack-runner:
    steps:
      - name: corepack runner
        run: corepack pnpm run build
  yarn-runner:
    steps:
      - name: yarn runner
        run: yarn dlx pnpm run build
  non-matches:
    steps:
      - run: echo pnpm-lock.yaml .pnpm-store @pnpm/exe PNPM_HOME
`);

  assert.deepEqual(diagnostics, [
    limitedPnpm(
      'bash-c',
      'bash command string',
      "bash -c 'pnpm run build'",
      'code-bearing bash',
    ),
    limitedPnpm('bash-s', 'bash stdin option', 'bash -s', 'code-bearing bash'),
    limitedPnpm(
      'here-string',
      'bash here string',
      "bash <<< 'pnpm run build'",
      'code-bearing bash',
    ),
    limitedPnpm('heredoc', 'bash heredoc', 'bash', 'code-bearing bash'),
    limitedPnpm('pipe', 'pipe to shell', 'sh', 'code-bearing sh'),
    missingPnpm('script-file', 'later consumer', 'pnpm run build'),
    limitedPnpm(
      'unknown-runners',
      'npx runner',
      'npx pnpm run build',
      'unmodelled npx invocation',
    ),
    limitedPnpm(
      'corepack-runner',
      'corepack runner',
      'corepack pnpm run build',
      'unmodelled corepack invocation',
    ),
    limitedPnpm(
      'yarn-runner',
      'yarn runner',
      'yarn dlx pnpm run build',
      'unmodelled yarn invocation',
    ),
  ]);
});

test('process substitutions and background terminators keep installs conditional', () => {
  const diagnostics = pnpmDiagnosticSummary(`jobs:
  argument-process-substitution:
    steps:
      - name: argument consumer
        run: diff <(pnpm run build) expected
  redirect-process-substitution:
    steps:
      - name: redirect consumer
        run: tee >(pnpm run build) < input
  background-install:
    steps:
      - name: background install
        run: pnpm install &
      - run: pnpm run build
  background-consumer:
    steps:
      - name: background consumer
        run: pnpm run build &
  brace-background:
    steps:
      - name: brace background
        run: '{ pnpm install & }'
  subshell-background:
    steps:
      - name: subshell background
        run: (pnpm install &)
`);

  assert.deepEqual(diagnostics, [
    missingPnpm(
      'argument-process-substitution',
      'argument consumer',
      'pnpm run build',
    ),
    missingPnpm(
      'redirect-process-substitution',
      'redirect consumer',
      'pnpm run build',
    ),
    limitedPnpm('background-install', 'background install', 'pnpm install'),
    missingPnpm('background-consumer', 'background consumer', 'pnpm run build'),
    limitedPnpm('brace-background', 'brace background', 'pnpm install'),
    limitedPnpm('subshell-background', 'subshell background', 'pnpm install'),
  ]);
});

test('documented pnpm install options distinguish narrowing from ordinary installs', () => {
  const narrowing = [
    '--global',
    '-g',
    '--lockfile-only',
    '--dir packages/example',
    '-C packages/example',
    '--prefix packages/example',
    '--filter pkg',
    '-F pkg',
    '--ignore-workspace',
    '--prod',
    '-P',
    '--production',
    '--dev',
    '-D',
    '--no-optional',
  ];
  for (const [index, option] of narrowing.entries()) {
    for (const command of [
      `pnpm ${option} install`,
      `pnpm install ${option}`,
    ]) {
      const job = `narrow-${index}`;
      assert.deepEqual(
        pnpmDiagnosticSummary(`jobs:
  ${job}:
    steps:
      - run: ${command}
      - name: later consumer
        run: pnpm run build
`),
        [missingPnpm(job, 'later consumer', 'pnpm run build')],
        command,
      );
    }
  }

  const establishing = [
    'pnpm install -w --ignore-scripts --offline --force --frozen-lockfile',
    'pnpm add -D -w --save-exact package',
    'pnpm add -Dw --save-exact package',
  ];
  for (const [index, command] of establishing.entries()) {
    assert.deepEqual(
      pnpmDiagnosticSummary(`jobs:
  ordinary-${index}:
    steps:
      - run: ${command}
      - run: pnpm run build
`),
      [],
      command,
    );
  }

  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
  unknown-before:
    steps:
      - name: unknown before subcommand
        run: pnpm --future-option value install
  unknown-after:
    steps:
      - name: unknown after subcommand
        run: pnpm install --future-option value
  nonliteral-before:
    steps:
      - name: nonliteral before subcommand
        run: pnpm "$COMMAND" run build
`),
    [
      limitedPnpm(
        'unknown-before',
        'unknown before subcommand',
        'pnpm --future-option value install',
        'unmodelled pnpm option --future-option',
      ),
      limitedPnpm(
        'unknown-after',
        'unknown after subcommand',
        'pnpm install --future-option value',
        'unmodelled pnpm option --future-option',
      ),
      limitedPnpm(
        'nonliteral-before',
        'nonliteral before subcommand',
        'pnpm "$COMMAND" run build',
        'non-literal pnpm argument',
      ),
    ],
  );
});

test('working-directory precedence controls whether an install is workspace-wide', () => {
  const diagnostics = pnpmDiagnosticSummary(`defaults:
  run:
    working-directory: packages/workflow
jobs:
  default-root:
    defaults:
      run:
        working-directory: .
    steps:
      - run: pnpm install
      - run: pnpm run build
  step-root:
    defaults:
      run:
        working-directory: packages/job
    steps:
      - working-directory: ./
        run: pnpm install
      - run: pnpm run build
  step-non-root:
    defaults:
      run:
        working-directory: .
    steps:
      - name: step directory
        working-directory: packages/example
        run: pnpm install
  job-non-root:
    defaults:
      run:
        working-directory: packages/example
    steps:
      - name: job directory
        run: pnpm install
  workflow-non-root:
    steps:
      - name: workflow directory
        run: pnpm install
  expression-directory:
    defaults:
      run:
        working-directory: .
    steps:
      - name: expression directory
        working-directory: \${{ github.workspace }}
        run: pnpm install
`);

  assert.deepEqual(diagnostics, [
    limitedPnpm('step-non-root', 'step directory', 'pnpm install'),
    limitedPnpm('job-non-root', 'job directory', 'pnpm install'),
    limitedPnpm('workflow-non-root', 'workflow directory', 'pnpm install'),
    limitedPnpm('expression-directory', 'expression directory', 'pnpm install'),
  ]);

  const defaults = [
    [
      'job-root',
      `jobs:
  job-root:
    defaults:
      run:
        working-directory: .
    steps:
      - run: pnpm install
      - run: pnpm run build`,
      [],
    ],
    [
      'job-expression',
      `jobs:
  job-expression:
    defaults:
      run:
        working-directory: \${{ github.workspace }}
    steps:
      - name: install
        run: pnpm install`,
      [limitedPnpm('job-expression', 'install', 'pnpm install')],
    ],
    [
      'workflow-root',
      `defaults:
  run:
    working-directory: .
jobs:
  workflow-root:
    steps:
      - run: pnpm install
      - run: pnpm run build`,
      [],
    ],
    [
      'workflow-expression',
      `defaults:
  run:
    working-directory: \${{ github.workspace }}
jobs:
  workflow-expression:
    steps:
      - name: install
        run: pnpm install`,
      [limitedPnpm('workflow-expression', 'install', 'pnpm install')],
    ],
  ];
  for (const [label, workflow, expected] of defaults) {
    assert.deepEqual(pnpmDiagnosticSummary(workflow), expected, label);
  }
});

test('directory and shell-option mutations prevent later installs from establishing', () => {
  const rows = [
    ['cd', 'cd packages/example'],
    ['pushd', 'pushd packages/example'],
    ['popd', 'popd'],
    ['set-plus-e', 'set +e'],
    ['set-other-option', 'set -o vi'],
    ['set-positional', 'set -- value'],
    ['shopt', 'shopt -s inherit_errexit'],
  ];
  for (const [job, setup] of rows) {
    assert.deepEqual(
      pnpmDiagnosticSummary(`jobs:
  ${job}:
    steps:
      - name: changed shell state
        run: |
          ${setup}
          pnpm install
`),
      [limitedPnpm(job, 'changed shell state', 'pnpm install')],
      job,
    );
  }

  for (const [job, setup] of [
    ['set-minus-e', 'set -e'],
    ['set-minus-u', 'set -u'],
    ['set-minus-x', 'set -x'],
    ['set-combined', 'set -euo pipefail'],
    ['set-errexit', 'set -o errexit'],
    ['set-nounset', 'set -o nounset'],
    ['set-pipefail', 'set -o pipefail'],
  ]) {
    assert.deepEqual(
      pnpmDiagnosticSummary(`jobs:
  ${job}:
    steps:
      - run: |
          ${setup}
          pnpm install
          pnpm run build
`),
      [],
      job,
    );
  }
});

test('standard shells establish installs and custom Bash templates do not', () => {
  const diagnostics = pnpmDiagnosticSummary(`jobs:
  default-shell:
    steps:
      - run: pnpm install
      - run: pnpm run build
  exact-bash:
    steps:
      - shell: bash
        run: pnpm install
      - run: pnpm run build
  exact-sh:
    steps:
      - shell: sh
        run: pnpm install
      - run: pnpm run build
  custom-with-e:
    steps:
      - name: custom Bash with errexit
        shell: /usr/bin/bash -e {0}
        run: pnpm install
  custom-without-e:
    steps:
      - name: custom Bash without errexit
        shell: /usr/bin/bash {0}
        run: pnpm install
`);

  assert.deepEqual(diagnostics, [
    limitedPnpm('custom-with-e', 'custom Bash with errexit', 'pnpm install'),
    limitedPnpm(
      'custom-without-e',
      'custom Bash without errexit',
      'pnpm install',
    ),
  ]);
});

test('list facts follow successful exits and export from the final statement', () => {
  const diagnostics = pnpmDiagnosticSummary(`jobs:
  final-and:
    steps:
      - run: pnpm install && pnpm run build
      - run: pnpm run test
  non-final-and:
    steps:
      - name: non-final list
        run: |
          pnpm install && pnpm run build
          echo done
  fallback-success:
    steps:
      - run: pnpm install || true
      - name: after fallback
        run: pnpm run build
  failure-arm-cannot-succeed:
    steps:
      - run: pnpm install || false
      - run: pnpm run build
  all-success-paths-install:
    steps:
      - run: false && pnpm install || pnpm install
      - run: pnpm run build
  some-success-paths-skip:
    steps:
      - run: pnpm install && false || true
      - name: after joined paths
        run: pnpm run build
  compound-left:
    steps:
      - name: compound left operand
        run: "{ pnpm install; true; } && pnpm run build"
  if-left:
    steps:
      - name: if left operand
        run: "if true; then pnpm install; true; fi && pnpm run build"
  case-left:
    steps:
      - name: case left operand
        run: "case x in x) pnpm install; true ;; esac && pnpm run build"
`);

  assert.deepEqual(diagnostics, [
    limitedPnpm('non-final-and', 'non-final list', 'pnpm install'),
    missingPnpm('fallback-success', 'after fallback', 'pnpm run build'),
    missingPnpm(
      'some-success-paths-skip',
      'after joined paths',
      'pnpm run build',
    ),
    limitedPnpm('compound-left', 'compound left operand', 'pnpm install'),
    limitedPnpm('if-left', 'if left operand', 'pnpm install'),
    limitedPnpm('case-left', 'case left operand', 'pnpm install'),
  ]);
});

test('comments do not change list finality and multiline expressions stay non-literal', () => {
  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
  trailing-comment:
    steps:
      - run: |
          pnpm install && pnpm run build
          # The list remains the final statement.
  interleaved-comment:
    steps:
      - run: |
          pnpm install &&
          # The comment is not a list operand.
          pnpm run build
  multiline-expression:
    steps:
      - name: expression install
        run: |
          pnpm install \${{
            inputs.flags
          }}
`),
    [
      limitedPnpm(
        'multiline-expression',
        'expression install',
        ['pnpm install $', '{{\n  inputs.flags\n}}'].join(''),
        'non-literal pnpm argument',
      ),
    ],
  );
});

test('if and case export branch intersections and reject condition installs', () => {
  const diagnostics = pnpmDiagnosticSummary(`jobs:
  exhaustive-case-alternative:
    steps:
      - run: |
          case "$RUNNER_OS" in
            Linux) pnpm install ;;
            Darwin|*) pnpm i ;;
          esac
          pnpm run build
  case-fallthrough:
    steps:
      - name: fallthrough case
        run: |
          case "$RUNNER_OS" in
            Linux) pnpm install ;&
            *) pnpm i ;;
          esac
  if-condition:
    steps:
      - name: if condition install
        run: if pnpm install; then pnpm run build; fi
  elif-condition:
    steps:
      - name: elif condition install
        run: if false; then true; elif pnpm install; then pnpm run build; fi
  while-condition:
    steps:
      - name: while condition install
        run: while pnpm install; do pnpm run build; done
  until-condition:
    steps:
      - name: until condition install
        run: until pnpm install; do pnpm run build; done
  case-selector:
    steps:
      - name: case selector install
        run: case $(pnpm install) in value) pnpm run build ;; esac
`);

  assert.deepEqual(diagnostics, [
    limitedPnpm(
      'case-fallthrough',
      'fallthrough case',
      `case "$RUNNER_OS" in
  Linux) pnpm install ;&
  *) pnpm i ;;
esac`,
      'case fallthrough',
    ),
    limitedPnpm('if-condition', 'if condition install', 'pnpm install'),
    limitedPnpm('elif-condition', 'elif condition install', 'pnpm install'),
    limitedPnpm('while-condition', 'while condition install', 'pnpm install'),
    limitedPnpm('until-condition', 'until condition install', 'pnpm install'),
    limitedPnpm('case-selector', 'case selector install', 'pnpm install'),
  ]);
});

test('later-step status conditions do not inherit a prior install fact', () => {
  const rows = [
    ['always', githubExpression('always()')],
    ['failure', githubExpression('failure()')],
    ['cancelled', githubExpression('cancelled()')],
    ['not-cancelled', githubExpression('!cancelled()')],
    ['unreadable', githubExpression('matrix.run_consumer')],
  ];
  for (const [job, condition] of rows) {
    assert.deepEqual(
      pnpmDiagnosticSummary(`jobs:
  ${job}:
    steps:
      - run: pnpm install
      - name: conditional consumer
        if: ${condition}
        run: pnpm run build
`),
      [
        limitedPnpm(
          job,
          'conditional consumer',
          condition,
          'step condition is not stable',
        ),
      ],
      job,
    );
  }

  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
  stable:
    steps:
      - run: pnpm install
      - name: stable consumer
        if: \${{ github.event_name == 'push' }}
        run: pnpm run build
`),
    [],
  );
});

test('the first limit stops a job and established installs suppress later limits', () => {
  const diagnostics = pnpmDiagnosticSummary(`jobs:
  first-limit:
    steps:
      - name: unknown runner
        run: npx pnpm run build
      - name: later consumer
        run: pnpm run test
  installed-first:
    steps:
      - run: pnpm install
      - run: npx pnpm run build
      - run: eval 'pnpm run test'
  installed-before-background:
    steps:
      - run: pnpm install
      - run: pnpm install &
`);

  assert.deepEqual(diagnostics, [
    limitedPnpm(
      'first-limit',
      'unknown runner',
      'npx pnpm run build',
      'unmodelled npx invocation',
    ),
  ]);
});

test('continue-on-error expressions make action and shell installs unknown', () => {
  const diagnostics = pnpmDiagnostics(`jobs:
  shell-expression:
    steps:
      - name: shell expression
        continue-on-error: \${{ matrix.allow_failure }}
        run: pnpm install
      - run: pnpm run build
  action-expression:
    steps:
      - name: action expression
        uses: pnpm/action-setup@v4
        continue-on-error: \${{ matrix.allow_failure }}
        with: { run_install: true }
      - run: pnpm run build
  action-condition-expression:
    steps:
      - name: action condition expression
        if: \${{ steps.probe.outcome == 'success' }}
        uses: pnpm/action-setup@v4
        with: { run_install: true }
      - run: pnpm run build
  shell-false:
    steps:
      - continue-on-error: false
        run: pnpm install
      - run: pnpm run build
  action-absent:
    steps:
      - uses: pnpm/action-setup@v4
        with: { run_install: true }
      - run: pnpm run build
  action-tolerated:
    steps:
      - continue-on-error: true
        uses: pnpm/action-setup@v4
        with: { run_install: true }
      - name: consumer after tolerated action
        run: pnpm run build
`);

  assert.deepEqual(
    diagnostics.map(({ code, message }) => ({ code, message })),
    [
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `shell-expression` step `shell expression` cannot be shown to run pnpm only after an install (continue-on-error is not literal): $' +
          '{{ matrix.allow_failure }}',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `action-expression` step `action expression` cannot be shown to run pnpm only after an install (continue-on-error is not literal): $' +
          '{{ matrix.allow_failure }}',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `action-condition-expression` step `action condition expression` cannot be shown to run pnpm only after an install (step condition is not stable): $' +
          "{{ steps.probe.outcome == 'success' }}",
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `action-tolerated` invokes pnpm in `consumer after tolerated action` with no earlier install step: pnpm run build',
      },
    ],
  );
});

test('unknown conditions use parsed Bash commands and established installs gate limits', () => {
  const diagnostics = pnpmDiagnostics(`jobs:
  unknown-condition-data:
    steps:
      - if: \${{ steps.probe.outcome == 'success' }}
        run: |
          printf '%s' 'pnpm run quoted'
          # pnpm run commented
          cat <<'TEXT'
          pnpm run heredoc
          TEXT
  limit-after-install:
    steps:
      - run: pnpm install
      - name: eval after install
        run: eval 'pnpm run build'
`);

  assert.deepEqual(diagnostics, []);
});

test('step, job, and workflow shell selection use GitHub precedence', () => {
  const diagnostics = pnpmDiagnostics(`defaults:
  run:
    shell: pwsh
jobs:
  workflow-default:
    steps:
      - name: workflow shell
        run: pnpm install
  job-default:
    defaults:
      run:
        shell: cmd
    steps:
      - name: job shell
        run: pnpm install
  step-override:
    defaults:
      run:
        shell: pwsh
    steps:
      - name: explicit Bash
        shell: bash
        run: pnpm run build
`);

  assert.deepEqual(
    diagnostics.map(({ code, message }) => ({ code, message })),
    [
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `workflow-default` step `workflow shell` cannot be shown to run pnpm only after an install (step shell is not Bash-compatible): pwsh',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `job-default` step `job shell` cannot be shown to run pnpm only after an install (step shell is not Bash-compatible): cmd',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `step-override` invokes pnpm in `explicit Bash` with no earlier install step: pnpm run build',
      },
    ],
  );
});

test('action setup inputs and unsupported command indirection fail conservatively', () => {
  const diagnostics = pnpmDiagnostics(`jobs:
  action-true:
    steps:
      - uses: pnpm/action-setup@v4
        with: { run_install: true }
      - run: pnpm run build
  action-false:
    steps:
      - uses: pnpm/action-setup@v4
        with: { run_install: 'false' }
      - name: false input consumer
        run: pnpm run build
  action-empty:
    steps:
      - uses: pnpm/action-setup@v4
        with: { run_install: [] }
      - name: empty input consumer
        run: pnpm run build
  action-null:
    steps:
      - uses: pnpm/action-setup@v4
        with: { run_install: null }
      - name: null input consumer
        run: pnpm run build
  action-config:
    steps:
      - uses: pnpm/action-setup@v4
        with:
          run_install:
            - args: [--frozen-lockfile]
      - run: pnpm run build
  loose-prefix:
    steps:
      - uses: pnpm/action-setup-helper@v4
        with: { run_install: true }
      - name: loose prefix consumer
        run: pnpm run build
  function:
    steps:
      - name: function indirection
        run: install_deps() { pnpm install; }
  eval:
    steps:
      - name: eval indirection
        run: eval 'pnpm install'
  dynamic:
    steps:
      - name: dynamic executable
        run: \${RUNNER:-pnpm} install
  other-shell:
    steps:
      - name: powershell command
        shell: pwsh
        run: pnpm install
`);

  assert.deepEqual(
    diagnostics.map(({ code, message }) => ({ code, message })),
    [
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `action-false` invokes pnpm in `false input consumer` with no earlier install step: pnpm run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `action-empty` invokes pnpm in `empty input consumer` with no earlier install step: pnpm run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `action-null` invokes pnpm in `null input consumer` with no earlier install step: pnpm run build',
      },
      {
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `loose-prefix` invokes pnpm in `loose prefix consumer` with no earlier install step: pnpm run build',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `function` step `function indirection` cannot be shown to run pnpm only after an install (code-bearing function_definition): install_deps() { pnpm install; }',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          "job `eval` step `eval indirection` cannot be shown to run pnpm only after an install (code-bearing eval): eval 'pnpm install'",
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `dynamic` step `dynamic executable` cannot be shown to run pnpm only after an install (non-literal executable): $' +
          '{RUNNER:-pnpm} install',
      },
      {
        code: 'PNPM_ANALYSIS_LIMIT',
        message:
          'job `other-shell` step `powershell command` cannot be shown to run pnpm only after an install (step shell is not Bash-compatible): pwsh',
      },
    ],
  );
});

test('pnpm/action-setup run_install entries establish only a root full install', () => {
  const action = 'pnpm/action-setup@b906affcce14559ad1aafd4ab0e942779e9f58b1';
  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
  lockfile-only:
    steps:
      - name: narrow action
        uses: ${action}
        with:
          run_install: |
            args: [--lockfile-only]
      - name: later consumer
        run: pnpm run build
  production-only:
    steps:
      - uses: ${action}
        with:
          run_install:
            args: [--prod]
      - name: later consumer
        run: pnpm run build
  filtered:
    steps:
      - uses: ${action}
        with:
          run_install:
            args: [--filter, pkg]
      - name: later consumer
        run: pnpm run build
  global:
    steps:
      - uses: ${action}
        with:
          run_install:
            args: [--global]
      - name: later consumer
        run: pnpm run build
  non-root:
    steps:
      - uses: ${action}
        with:
          run_install:
            cwd: packages/example
      - name: later consumer
        run: pnpm run build
  narrow-array:
    steps:
      - uses: ${action}
        with:
          run_install:
            - args: [--lockfile-only]
            - cwd: packages/example
      - name: later consumer
        run: pnpm run build
  array-with-full-install:
    steps:
      - uses: ${action}
        with:
          run_install:
            - args: [--lockfile-only]
            - recursive: true
              args: [--frozen-lockfile]
            - unexpected: value
      - run: pnpm run build
  block-array-with-full-install:
    steps:
      - uses: ${action}
        with:
          run_install: |
            - args: [--lockfile-only]
            - recursive: true
              args: [--frozen-lockfile]
      - run: pnpm run build
  unreadable-key:
    steps:
      - name: unknown action key
        uses: ${action}
        with:
          run_install:
            unexpected: value
  unreadable-value:
    steps:
      - name: dynamic action args
        uses: ${action}
        with:
          run_install:
            args: ["${githubExpression('matrix.install_args')}"]
  unreadable-recursive:
    steps:
      - name: non-boolean recursive
        uses: ${action}
        with:
          run_install:
            recursive: yes
`),
    [
      missingPnpm('lockfile-only', 'later consumer', 'pnpm run build'),
      missingPnpm('production-only', 'later consumer', 'pnpm run build'),
      missingPnpm('filtered', 'later consumer', 'pnpm run build'),
      missingPnpm('global', 'later consumer', 'pnpm run build'),
      missingPnpm('non-root', 'later consumer', 'pnpm run build'),
      missingPnpm('narrow-array', 'later consumer', 'pnpm run build'),
      limitedPnpm(
        'unreadable-key',
        'unknown action key',
        action,
        'unmodelled pnpm/action-setup key unexpected',
      ),
      limitedPnpm(
        'unreadable-value',
        'dynamic action args',
        action,
        'non-literal pnpm/action-setup args',
      ),
      limitedPnpm(
        'unreadable-recursive',
        'non-boolean recursive',
        action,
        'non-literal pnpm/action-setup recursive value',
      ),
    ],
  );
});

test('tolerated steps retain their installation fact inside their own block', () => {
  const condition = githubExpression('matrix.allow_failure');
  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
  straight-line:
    steps:
      - continue-on-error: true
        run: |
          pnpm install --frozen-lockfile
          pnpm run build
  and-list:
    steps:
      - continue-on-error: true
        run: pnpm install --frozen-lockfile && pnpm run build
  dynamic-tolerance:
    steps:
      - name: dynamic tolerance
        continue-on-error: ${condition}
        run: |
          pnpm install --frozen-lockfile
          pnpm run build
`),
    [
      limitedPnpm(
        'dynamic-tolerance',
        'dynamic tolerance',
        condition,
        'continue-on-error is not literal',
      ),
    ],
  );
});

test('parse errors, argument limits, coproc, and exec option positions fail conservatively', () => {
  const parseError = `if true; then
  (
fi
pnpm install
pnpm build`;
  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
  root-error:
    steps:
      - name: malformed block
        run: |
${indentYamlBlock(parseError)}
  argument-limit:
    steps:
      - name: substitution before consumer
        run: pnpm build $(pnpm install)
  coprocess:
    steps:
      - name: coprocess definition
        run: coproc echo hi
      - run: pnpm run build
  no-pnpm-function:
    steps:
      - name: opaque function
        run: greet() { echo hi; }
  exec-argument:
    steps:
      - run: exec node build.js -a
      - name: later consumer
        run: pnpm run build
  exec-option:
    steps:
      - name: exec alternate name
        run: exec -a alternate /bin/pnpm install
  timeout-separator:
    steps:
      - run: timeout -- 10 pnpm install
      - run: pnpm run build
`),
    [
      limitedPnpm(
        'root-error',
        'malformed block',
        `${parseError}\n`,
        'Bash parse error',
      ),
      limitedPnpm(
        'argument-limit',
        'substitution before consumer',
        'pnpm install',
        'install is not guaranteed',
      ),
      missingPnpm('coprocess', 'step 2', 'pnpm run build'),
      missingPnpm('exec-argument', 'later consumer', 'pnpm run build'),
      limitedPnpm(
        'exec-option',
        'exec alternate name',
        'exec -a alternate /bin/pnpm install',
        'unmodelled wrapper option -a',
      ),
    ],
  );
});

test('pnpm-free code-bearing commands stay outside conditional-step limits', () => {
  const condition = githubExpression('matrix.enabled');
  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
  eval:
    steps:
      - if: ${condition}
        run: eval "$CMD"
  bash:
    steps:
      - if: ${condition}
        run: bash -c "$CMD"
`),
    [],
  );
});

test('backslashes in quoted and concatenated executables are non-literal', () => {
  const rows = [
    ['double-quoted', '"pn\\\npm" run build'],
    ['single-quoted', "'pn\\npm' run build"],
    ['concatenated', "p' n\\\\'pm run build".replace(' ', '')],
  ];
  for (const [job, command] of rows) {
    const source = withPnpmToken(command);
    assert.deepEqual(
      pnpmDiagnosticSummary(`jobs:
  ${job}:
    steps:
      - name: probe
        run: ${JSON.stringify(source)}
`),
      [limitedPnpm(job, 'probe', command, 'non-literal executable')],
      job,
    );
  }
});

test('the Bash dispatch and explicit unmodelled sets cover the pinned grammar', () => {
  const nodeTypes = JSON.parse(
    readFileSync(
      require.resolve('tree-sitter-bash/src/node-types.json'),
      'utf8',
    ),
  );
  const namedTypes = nodeTypes
    .filter(({ named }) => named)
    .map(({ type }) => type)
    .sort();
  const classifiedTypes = [
    ...new Set([...NODE_HANDLER_TYPES, ...UNMODELLED_NODE_TYPES]),
  ].sort();

  assert.deepEqual(classifiedTypes, namedTypes);
});

test('transparent wrapper context cross-product preserves command classification', () => {
  const wrappers = [
    ['env', 'env CI=1'],
    ['time', 'time'],
    ['command', 'command'],
    ['builtin', 'builtin'],
    ['exec', 'exec'],
    ['sudo', 'sudo'],
    ['nohup', 'nohup'],
    ['nice', 'nice'],
    ['timeout', 'timeout -- 10'],
    ['stdbuf', 'stdbuf -oL'],
    ['xargs', 'xargs -n 1'],
  ];
  const columns = [
    {
      label: 'consumer',
      source: (wrapper) => `${wrapper} pnpm run build`,
      reason: undefined,
      result: 'missing',
    },
    {
      label: 'install',
      source: (wrapper) => `${wrapper} pnpm install`,
      reason: undefined,
      result: 'install',
    },
    {
      label: 'bash-c',
      source: (wrapper) => `${wrapper} bash -c 'pnpm run build'`,
      reason: 'code-bearing bash',
      result: 'limit',
    },
    {
      label: 'bash-here-string',
      source: (wrapper) => `${wrapper} bash <<< 'pnpm run build'`,
      reason: 'code-bearing bash',
      result: 'limit',
    },
    {
      label: 'sh-heredoc',
      source: (wrapper) => `${wrapper} sh <<'SCRIPT'\npnpm run build\nSCRIPT`,
      reportedCommand: (wrapper) => `${wrapper} sh`,
      reason: 'code-bearing sh',
      result: 'limit',
    },
    {
      label: 'sh-pipe',
      source: (wrapper) => withPnpmToken(`printf input | ${wrapper} sh`),
      reportedCommand: (wrapper) => `${wrapper} sh`,
      reason: 'code-bearing sh',
      result: 'limit',
    },
    {
      label: 'nonliteral-executable',
      source: (wrapper) => withPnpmToken(`${wrapper} $PM run build`),
      reportedCommand: (wrapper) => `${wrapper} $PM run build`,
      reason: 'non-literal executable',
      result: 'limit',
    },
  ];
  const jobs = [];
  const expected = [];
  for (const [wrapperName, wrapper] of wrappers) {
    for (const column of columns) {
      const job = `${wrapperName}-${column.label}`;
      const source = column.source(wrapper);
      jobs.push(`  ${job}:
    steps:
      - name: probe
        run: |
${indentYamlBlock(source)}${
  column.result === 'install'
    ? `
      - name: later consumer
        run: pnpm run build`
    : ''
}`);
      if (column.result === 'missing') {
        expected.push(missingPnpm(job, 'probe', source));
      } else if (column.result === 'limit') {
        const command = column.reportedCommand?.(wrapper) ?? source;
        expected.push(limitedPnpm(job, 'probe', command, column.reason));
      } else if (wrapperName === 'xargs') {
        expected.push(missingPnpm(job, 'later consumer', 'pnpm run build'));
      }
    }
  }
  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
${jobs.join('\n')}
`),
    expected,
  );
});

test('current-shell constructs export taints and child-shell constructs do not', () => {
  const taints = [
    ['cd', 'cd packages/example'],
    ['pushd', 'pushd packages/example'],
    ['set', 'set +e'],
    ['shopt', 'shopt -s nullglob'],
  ];
  const currentShellConstructs = [
    ['brace', (taint) => `{ ${taint}; }`],
    ['negation', (taint) => `! ${taint}`],
    ['for', (taint) => `for value in one; do ${taint}; done`],
    [
      'c-for',
      (taint) => `for ((index = 0; index < 1; index++)); do ${taint}; done`,
    ],
    ['while', (taint) => `while ${taint}; do break; done`],
    ['until', (taint) => `until ${taint}; do break; done`],
    ['if-condition', (taint) => `if ${taint}; then :; fi`],
    ['if-then', (taint) => `if true; then ${taint}; else :; fi`],
    [
      'if-elif',
      (taint) => `if false; then :; elif true; then ${taint}; else :; fi`,
    ],
    ['if-else', (taint) => `if false; then :; else ${taint}; fi`],
    ['case-arm', (taint) => `case value in value) ${taint} ;; esac`],
    ['list', (taint) => `${taint} && true`],
  ];
  const jobs = [];
  const expected = [];
  for (const [constructName, construct] of currentShellConstructs) {
    for (const [taintName, taint] of taints) {
      const job = `${constructName}-${taintName}`;
      jobs.push(`  ${job}:
    steps:
      - name: probe
        run: |
          ${construct(taint)}
          pnpm install`);
      expected.push(limitedPnpm(job, 'probe', 'pnpm install'));
    }
  }
  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
${jobs.join('\n')}
`),
    expected,
  );

  const childShellConstructs = [
    ['subshell', (taint) => `(${taint})`],
    ['pipeline', (taint) => `${taint} | true`],
    ['command-substitution', (taint) => `echo "$(${taint})"`],
    ['process-substitution', (taint) => `cat < <(${taint})`],
    ['background', (taint) => `${taint} &`],
  ];
  const childJobs = [];
  for (const [constructName, construct] of childShellConstructs) {
    for (const [taintName, taint] of taints) {
      childJobs.push(`  ${constructName}-${taintName}:
    steps:
      - run: |
          ${construct(taint)}
          pnpm install
          pnpm run build`);
    }
  }
  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
${childJobs.join('\n')}
`),
    [],
  );
});

test('install and add option policies are command-specific on both sides of the subcommand', () => {
  // The cross-product pins behavior mapping end to end; arity fidelity comes
  // from literal lists transcribed from the pinned pnpm declarations.
  const valueTakingOptions = {
    install: [
      '--changed-files-ignore-pattern',
      '--child-concurrency',
      '--dir',
      '--filter',
      '--filter-prod',
      '--global-dir',
      '--hoist-pattern',
      '--lockfile-dir',
      '--loglevel',
      '--modules-dir',
      '--network-concurrency',
      '--package-import-method',
      '--prefix',
      '--public-hoist-pattern',
      '--reporter',
      '--store-dir',
      '--test-pattern',
      '--trust-policy',
      '--trust-policy-exclude',
      '--trust-policy-ignore-after',
      '--virtual-store-dir',
      '--workspace-concurrency',
      '-C',
      '-F',
    ],
    add: [
      '--allow-build',
      '--changed-files-ignore-pattern',
      '--dir',
      '--filter',
      '--filter-prod',
      '--global-dir',
      '--loglevel',
      '--modules-dir',
      '--prefix',
      '--reporter',
      '--save-catalog-name',
      '--store-dir',
      '--test-pattern',
      '--virtual-store-dir',
      '--workspace-concurrency',
      '-C',
      '-F',
    ],
  };
  for (const subcommand of ['install', 'add']) {
    assert.deepEqual(
      PNPM_OPTION_POLICIES[subcommand]
        .filter(({ arity }) => arity === 1)
        .map(({ name }) => name)
        .sort(),
      valueTakingOptions[subcommand],
      `${subcommand} value-taking options`,
    );
  }
  const jobs = [];
  const expected = [];
  for (const subcommand of ['install', 'add']) {
    const options = PNPM_OPTION_POLICIES[subcommand];
    for (const [index, { name, arity, behavior }] of options.entries()) {
      const operand =
        behavior === 'directory'
          ? 'packages/example'
          : name === '--allow-build'
            ? 'esbuild'
            : 'value';
      const option = arity === 1 ? `${name} ${operand}` : name;
      for (const position of ['before', 'after']) {
        const job = `${subcommand}-${index}-${position}`;
        const command =
          position === 'before'
            ? `pnpm ${option} ${subcommand}${subcommand === 'add' ? ' package' : ''}`
            : `pnpm ${subcommand} ${option}${subcommand === 'add' ? ' package' : ''}`;
        jobs.push(`  ${job}:
    steps:
      - name: option probe
        run: ${command}
      - name: later consumer
        run: pnpm run build`);
        if (behavior === 'neutral' || behavior === 'directory') {
          expected.push(missingPnpm(job, 'later consumer', 'pnpm run build'));
        } else if (behavior === 'limit') {
          expected.push(
            limitedPnpm(
              job,
              'option probe',
              command,
              `pnpm ${subcommand} option ${name} is not establishing`,
            ),
          );
        }
      }
    }
  }
  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
${jobs.join('\n')}
`),
    expected,
  );
});

test('link-workspace-packages Boolean and deep forms remain diagnostic', () => {
  const rows = [
    [
      'install-before-bare',
      'pnpm --link-workspace-packages install',
      limitedPnpm(
        'install-before-bare',
        'option probe',
        'pnpm --link-workspace-packages install',
        'pnpm install option --link-workspace-packages is not establishing',
      ),
    ],
    [
      'install-after-bare',
      'pnpm install --link-workspace-packages',
      limitedPnpm(
        'install-after-bare',
        'option probe',
        'pnpm install --link-workspace-packages',
        'pnpm install option --link-workspace-packages is not establishing',
      ),
    ],
    [
      'install-before-deep',
      'pnpm --link-workspace-packages deep install',
      missingPnpm(
        'install-before-deep',
        'option probe',
        'pnpm --link-workspace-packages deep install',
      ),
    ],
    [
      'install-after-deep',
      'pnpm install --link-workspace-packages deep',
      limitedPnpm(
        'install-after-deep',
        'option probe',
        'pnpm install --link-workspace-packages deep',
        'pnpm install option --link-workspace-packages is not establishing',
      ),
    ],
    [
      'add-before-bare',
      'pnpm --link-workspace-packages add package',
      limitedPnpm(
        'add-before-bare',
        'option probe',
        'pnpm --link-workspace-packages add package',
        'pnpm add option --link-workspace-packages is not establishing',
      ),
    ],
    [
      'add-after-bare',
      'pnpm add --link-workspace-packages package',
      limitedPnpm(
        'add-after-bare',
        'option probe',
        'pnpm add --link-workspace-packages package',
        'pnpm add option --link-workspace-packages is not establishing',
      ),
    ],
  ];

  for (const [job, command, expected] of rows) {
    assert.deepEqual(
      pnpmDiagnosticSummary(`jobs:
  ${job}:
    steps:
      - name: option probe
        run: ${command}
`),
      [expected],
      job,
    );
  }
});

test('repository-root spellings agree across command, workflow, and action contexts', () => {
  const rootPaths = ['.', './', './/', './.', '././'];
  const jobs = [];
  for (const [pathIndex, rootPath] of rootPaths.entries()) {
    for (const [optionIndex, option] of ['--dir', '-C', '--prefix'].entries()) {
      jobs.push(`  command-${pathIndex}-${optionIndex}:
    steps:
      - run: pnpm install ${option} ${rootPath}
      - run: pnpm run build`);
    }
    jobs.push(`  working-directory-${pathIndex}:
    steps:
      - working-directory: ${rootPath}
        run: pnpm install
      - run: pnpm run build`);
    jobs.push(`  action-cwd-${pathIndex}:
    steps:
      - uses: pnpm/action-setup@b906affcce14559ad1aafd4ab0e942779e9f58b1
        with:
          run_install:
            cwd: ${rootPath}
      - run: pnpm run build`);
  }
  assert.deepEqual(
    pnpmDiagnosticSummary(`jobs:
${jobs.join('\n')}
`),
    [],
  );
});

// The cases below pin the tracked workflows rather than the checker. They live
// here because the checker already parses every .github workflow, and a suite
// of their own would add a verify-core step to run it.
test('every tracked workflow installs before it invokes pnpm', () => {
  const result = checkGithubYamlFiles(join(repositoryRoot, '.github'));

  assert.deepEqual(
    result.errors.filter(({ code }) =>
      ['MISSING_PNPM_INSTALL', 'PNPM_ANALYSIS_LIMIT'].includes(code),
    ),
    [],
  );
});

test('the verify-core job declares every run command in order', () => {
  const commands = readVerifyCoreJob()
    .steps.filter((step) => typeof step.run === 'string')
    .map((step) => step.run);
  assert.deepEqual(commands, [
    'pnpm install --frozen-lockfile',
    'pnpm github:check',
    'pnpm github:check:test',
    'pnpm lint',
    'pnpm typecheck',
    'pnpm test:without-direct-scenario',
    'pnpm build',
    'pnpm test:node-tools',
    'pnpm docs:check',
    'pnpm docs:check:test',
    'pnpm docs:api',
    'pnpm test:release-order',
    'pnpm test:release-invocation',
    'pnpm test:packed-breakwater',
    'pnpm test:packed-fleet-control',
    'pnpm test:packed-flowsafe-agent-host',
    'pnpm test:packed-flowsafe-provisioning',
    'pnpm --filter @proofoftech/flowsafe test:signals-client-export',
    'pnpm --filter @proofoftech/flowsafe typecheck:react18',
    'pnpm --filter showcase run react-doctor',
    'pnpm --filter @proofoftech/flowsafe spike:verify',
    'pnpm test:conformance-config',
    'pnpm conformance:verify',
  ]);
  assert.ok(
    commands.indexOf('pnpm test:node-tools') > commands.indexOf('pnpm build'),
    'scripts/entry-point.test.mjs mint cases need the Build step output',
  );
});

test('the node-tools script names both root Node suites', () => {
  const { scripts } = JSON.parse(
    readFileSync(join(repositoryRoot, 'package.json'), 'utf8'),
  );
  assert.match(scripts['test:node-tools'], /scripts\/entry-point\.test\.mjs/u);
  assert.match(
    scripts['test:node-tools'],
    /scripts\/baseline-recorder\.test\.mjs/u,
  );
});

test('the public verification lists mirror verify-core', () => {
  const commands = readVerifyCoreJob()
    .steps.filter((step) => typeof step.run === 'string')
    .map((step) => step.run);
  const contributorCommands = commands.map((command) =>
    command === 'pnpm test:without-direct-scenario' ? 'pnpm test' : command,
  );
  const maintainerCommands = contributorCommands.slice(1);

  assert.deepEqual(
    bashFenceAfter('CONTRIBUTING.md', 'The verification list below mirrors'),
    contributorCommands,
  );
  assert.deepEqual(
    bashFenceAfter('docs/maintainer-guide.md', '## Verification'),
    maintainerCommands,
  );
});

test('the canary captures checked-in pins before updating dependencies', () => {
  const run = canaryStep('mastra_versions').run;
  const update = run.indexOf('pnpm -r update');

  for (const binding of ['BASELINE_CORE=', 'BASELINE_D1=']) {
    const capture = run.indexOf(binding);
    assert.ok(capture >= 0 && capture < update, binding);
  }
  assert.match(run, /baseline_core=\$BASELINE_CORE/u);
  assert.match(run, /baseline_d1=\$BASELINE_D1/u);
});

test('the canary summary distinguishes equality from forward-version coverage', () => {
  const core = JSON.parse(
    readFileSync(
      join(repositoryRoot, 'packages/breakwater/package.json'),
      'utf8',
    ),
  ).devDependencies['@mastra/core'];
  const d1 = JSON.parse(
    readFileSync(
      join(repositoryRoot, 'packages/flowsafe/package.json'),
      'utf8',
    ),
  ).dependencies['@mastra/cloudflare-d1'];
  const equal = runMastraVersionAssertion({
    baselineCore: core,
    baselineD1: d1,
    expectedCore: core,
    expectedD1: d1,
    installedCore: core,
    installedD1: d1,
  });
  assert.equal(equal.run.error, undefined);
  assert.equal(equal.run.status, 0, `${equal.run.stdout}\n${equal.run.stderr}`);
  assert.equal(
    equal.summary,
    `core: baseline=${core} requested=${core} installed=${core} coverage=equality\nd1: baseline=${d1} requested=${d1} installed=${d1} coverage=equality\n`,
  );

  const forward = runMastraVersionAssertion({
    baselineCore: core,
    baselineD1: d1,
    expectedCore: '1.68.0',
    expectedD1: '1.4.0',
    installedCore: '1.68.0',
    installedD1: '1.4.0',
  });
  assert.equal(forward.run.error, undefined);
  assert.equal(forward.run.status, 0, forward.run.stderr);
  assert.match(forward.summary, /core: .* coverage=newer/u);
  assert.match(forward.summary, /d1: .* coverage=newer/u);
});

test('the canary refuses mismatched and missing installed versions', () => {
  for (const [installedCore, installedD1] of [
    ['1.67.0', '1.4.0'],
    ['', '1.4.0'],
    ['1.68.0', '1.3.2'],
    ['1.68.0', ''],
  ]) {
    const result = runMastraVersionAssertion({
      baselineCore: '1.67.0',
      baselineD1: '1.3.2',
      expectedCore: '1.68.0',
      expectedD1: '1.4.0',
      installedCore,
      installedD1,
    });
    assert.equal(result.run.error, undefined);
    assert.ok(result.run.status > 0);
    assert.match(result.summary, /baseline=1\.67\.0 requested=1\.68\.0/u);
  }
});

test('the canary diagnoses invalid and older requested versions', () => {
  const older = runMastraVersionAssertion({
    baselineCore: '1.67.0',
    baselineD1: '1.3.2',
    expectedCore: '1.66.0',
    expectedD1: '1.4.0',
    installedCore: '1.66.0',
    installedD1: '1.4.0',
  });
  assert.ok(older.run.status > 0);
  assert.match(
    older.run.stderr,
    /requested 1\.66\.0 is older than baseline 1\.67\.0/u,
  );

  const invalid = runMastraVersionAssertion({
    baselineCore: '^1.67.0',
    baselineD1: '1.3.2',
    expectedCore: '1.68.0',
    expectedD1: '1.4.0',
    installedCore: '1.68.0',
    installedD1: '1.4.0',
  });
  assert.ok(invalid.run.status > 0);
  assert.match(
    invalid.run.stderr,
    /unusable version pair \^1\.67\.0 \/ 1\.68\.0/u,
  );
});

// This one reads the gate's shape and shells out to nothing, so it reports the
// deletions below on every machine.
test('the ci.yml gate job stays reachable and depends on its required jobs', () => {
  const { job } = readVerifyGateJob();

  assert.equal(
    job.if,
    'always()',
    'without `if: always()` a failed dependency skips the gate job, and GitHub reports a skipped required check as success',
  );
  assert.deepEqual(job.needs, ['verify-core', 'direct-scenario']);
  assert.ok(
    !job.needs.includes('mastra-compat'),
    'the compat canary reports an upstream release, so naming it here would block merges on an upstream red',
  );
});

// The canary's step-level `continue-on-error` keys keep an expected upstream
// red from skipping the tripwire suites after them, so the final step is where
// those outcomes reach the job status. This case runs that step's own body.
test('the ci.yml canary reds the job from a final outcome step that reads the ids it pins', () => {
  const job = readCompatCanaryJob();

  assert.ok(
    !('continue-on-error' in job),
    'a job-level `continue-on-error` reports the canary green however the probe went',
  );

  const steps = job.steps ?? [];
  for (const [name, id] of [
    ['Typecheck libraries against newest core', 'typecheck'],
    ['Bundle the flowsafe spike Worker against newest core', 'bundle'],
  ]) {
    const step = steps.find((candidate) => candidate.name === name);

    assert.ok(step, `the canary runs a step named "${name}"`);
    assert.equal(
      step['continue-on-error'],
      true,
      `without the step-level key on "${name}" its expected red skips the tripwire suites after it`,
    );
    assert.equal(
      step.id,
      id,
      `the outcome step reads steps.${id}.outcome, so "${name}" carries that id`,
    );
  }

  const testIndex = steps.findIndex((step) => step.id === 'tests');
  assert.ok(
    testIndex >= 0,
    'the canary runs its tripwire suites in a step with the id `tests`',
  );

  const outcomeIndex = steps.findIndex((step) => step.id === 'probe_outcome');
  assert.equal(
    outcomeIndex,
    steps.length - 1,
    'a step after the outcome step leaves its own red unreported',
  );
  assert.ok(
    outcomeIndex > testIndex,
    'the outcome step runs after the tripwire suites',
  );

  const outcome = steps[outcomeIndex];
  assert.equal(
    outcome.if,
    '$' + '{{ !cancelled() }}',
    'the outcome step reports completed probes without running after cancellation',
  );
  const expectedOutcomes = [
    ...steps
      .filter((step) => step['continue-on-error'] === true)
      .map((step) => step.id),
    steps[testIndex].id,
  ].sort();
  assert.deepEqual(
    [...new Set(outcomeReferences(outcome))].sort(),
    expectedOutcomes,
    'the outcome step reports the continue-on-error probes and the tripwire suites',
  );

  for (const [outcomes, succeeds] of [
    [{ typecheck: 'failure', bundle: 'success', tests: 'success' }, false],
    [{ typecheck: 'success', bundle: 'failure', tests: 'success' }, false],
    [{ typecheck: 'success', bundle: 'success', tests: 'failure' }, true],
    [{ typecheck: 'success', bundle: 'success', tests: 'success' }, true],
  ]) {
    const label = JSON.stringify(outcomes);
    const { run, summary } = runProbeOutcome(outcome, outcomes);

    assert.equal(run.error, undefined, label);
    if (succeeds) {
      assert.equal(run.status, 0, label);
    } else {
      assert.ok(run.status > 0, label);
    }
    assert.deepEqual(
      summary
        .split('\n')
        .filter((line) => line !== '')
        .map((line) => line.split(': ').at(-1)),
      [outcomes.typecheck, outcomes.bundle, outcomes.tests],
      label,
    );
  }
});

const NEWEST_VERSION_CASES = [
  {
    title: 'prints the newest version from one lookup',
    stub: NPM_STUBS.newest,
    lookups: 1,
    sleeps: 0,
    succeeds: true,
    stdout: '1.67.0\n',
  },
  {
    title: 'retries a registry blip and prints the version it then reads',
    stub: NPM_STUBS.blipThenNewest,
    lookups: 2,
    sleeps: 1,
    succeeds: true,
    stdout: '1.67.0\n',
  },
  {
    title: 'gives up on an unreachable registry after its last attempt',
    stub: NPM_STUBS.unreachable,
    lookups: 3,
    sleeps: 2,
    succeeds: false,
    stdout: '',
  },
  {
    title: 'takes an E404 as the permanent answer it is',
    stub: NPM_STUBS.missing,
    lookups: 1,
    sleeps: 0,
    succeeds: false,
    stdout: '',
  },
  {
    title: 'refuses a body it cannot reduce to a version',
    stub: NPM_STUBS.unparsable,
    lookups: 1,
    sleeps: 0,
    succeeds: false,
    stdout: '',
  },
  {
    title: 'refuses an empty reduced version',
    stub: NPM_STUBS.empty,
    lookups: 1,
    sleeps: 0,
    succeeds: false,
    stdout: '',
  },
  {
    title: 'reads a version npm answered alongside a warning on stderr',
    stub: NPM_STUBS.newestWithWarning,
    lookups: 1,
    sleeps: 0,
    succeeds: true,
    stdout: '1.67.0\n',
  },
];

for (const scenario of NEWEST_VERSION_CASES) {
  test(`the ci.yml canary lookup ${scenario.title}`, () => {
    const run = runNewestVersion(scenario.stub);

    assert.equal(run.error, undefined);
    assert.equal(run.lookups.length, scenario.lookups);
    assert.equal(run.timeouts.length, scenario.lookups);
    assert.equal(run.sleeps, scenario.sleeps);
    assert.equal(run.stdout, scenario.stdout);
    if (scenario.succeeds) {
      assert.equal(run.status, 0);
    } else {
      assert.ok(run.status > 0, 'the bump step carries the lookup failure');
    }
  });
}

test('the ci.yml canary retries a killed partial E404 before accepting success', () => {
  const run = runNewestVersion(NPM_STUBS.e404ThenNewest);

  assert.equal(run.error, undefined);
  assert.equal(run.status, 0);
  assert.equal(run.lookups.length, 2);
  assert.equal(run.timeouts.length, 2);
  assert.equal(run.sleeps, 1);
  assert.equal(run.stdout, '1.67.0\n');
});

test('the ci.yml canary exhausts killed partial E404 lookups', () => {
  const run = runNewestVersion(NPM_STUBS.repeatedKilledE404);

  assert.equal(run.error, undefined);
  assert.ok(run.status > 0);
  assert.equal(run.lookups.length, 3);
  assert.equal(run.timeouts.length, 3);
  assert.equal(run.sleeps, 2);
  assert.equal(run.stdout, '');
});

test('the ci.yml canary retries completed non-1 E404 responses', () => {
  const run = runNewestVersion(
    npmStub(`printf 'npm error code E404\\n'`, 'exit 2'),
  );

  assert.equal(run.error, undefined);
  assert.ok(run.status > 0);
  assert.equal(run.lookups.length, 3);
  assert.equal(run.sleeps, 2);
  assert.match(run.stderr, /npm view .* failed: npm error code E404/u);
});

for (const status of [125, 126, 127]) {
  test(`the ci.yml canary refuses timeout-wrapper status ${status} without retrying`, () => {
    const run = runNewestVersion(
      npmStub(`printf 'npm error code E404\\n'`, `exit ${status}`),
    );

    assert.equal(run.error, undefined);
    assert.ok(run.status > 0);
    assert.equal(run.lookups.length, 1);
    assert.equal(run.timeouts.length, 1);
    assert.equal(run.sleeps, 0);
    assert.equal(run.stdout, '');
    assert.match(
      run.stderr,
      new RegExp(`timeout wrapper failed with status ${status}`, 'u'),
    );
  });
}

test("the ci.yml canary lookup bounds npm's own fetch retries", () => {
  const run = runNewestVersion(NPM_STUBS.newest);

  assert.equal(run.lookups.length, 1);
  assert.match(run.timeouts[0], /^timeout --signal=KILL 90s npm view /u);
  for (const flag of [
    '--fetch-timeout=30000',
    '--fetch-retries=1',
    '--fetch-retry-mintimeout=1000',
    '--fetch-retry-maxtimeout=5000',
  ]) {
    assert.ok(
      run.lookups[0].includes(flag),
      `without ${flag} one attempt can spend the job budget inside npm: ${run.lookups[0]}`,
    );
  }
});

// At the current prerequisite count of two, the allocation keeps nine minutes
// beyond the wait, probe, and policy reserve. A third prerequisite forces a
// timeout-minutes decision.
test('the release job budget outlasts every prerequisite visibility wait', () => {
  const release = parse(
    readFileSync(join(repositoryRoot, '.github/workflows/release.yml'), 'utf8'),
  ).jobs.release;

  assert.ok(release, 'the release workflow publishes from the job `release`');
  const reserve = 15 * 60_000;
  const requiredBudget = (prerequisiteCount) =>
    prerequisiteCount * (VISIBILITY_DEADLINE_MS + PROBE_TIMEOUT_MS) +
    prerequisiteCount * PROBE_TIMEOUT_MS +
    reserve;
  const allocation = release['timeout-minutes'] * 60_000;
  assert.ok(allocation > requiredBudget(PUBLISH_PREREQUISITES.length));
  assert.equal(PUBLISH_PREREQUISITES.length, 2);
  assert.equal(allocation - requiredBudget(2), 9 * 60_000);
  assert.ok(allocation < requiredBudget(3));
});

// `bash -e -c` reproduces the runner's default shell for a `run` block that
// declares no `shell:` key, and the env variable the script reads is taken from
// the step rather than restated, so the wiring at ci.yml is what runs here. The
// job's single-run-step shape is asserted in readVerifyGateJob above.
test('the ci.yml gate rejects an empty or non-success needs context', (t) => {
  if (spawnSync('jq', ['--version']).status !== 0) {
    t.skip('jq is not on PATH, so the gate assertion cannot be evaluated');
    return;
  }

  const { step } = readVerifyGateJob();
  const cases = [
    { needs: {}, succeeds: false },
    {
      needs: {
        'verify-core': { result: 'success' },
        'direct-scenario': { result: 'success' },
      },
      succeeds: true,
    },
    ...['failure', 'cancelled', 'skipped'].map((result) => ({
      needs: {
        'verify-core': { result: 'success' },
        'direct-scenario': { result },
      },
      succeeds: false,
    })),
  ];

  for (const { needs, succeeds } of cases) {
    const label = JSON.stringify(needs);
    const run = runVerifyGate(step, needs);

    assert.equal(run.error, undefined, label);
    if (succeeds) {
      assert.equal(run.status, 0, label);
    } else {
      assert.ok(run.status > 0, label);
    }
    assert.deepEqual(
      run.stdout.split('\n').filter((line) => line !== ''),
      resultListing(needs),
      label,
    );
  }
});
