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
  PUBLISH_PREREQUISITES,
  VISIBILITY_DEADLINE_MS,
} from './publish-ordered.mjs';

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const temporaryDirectories = [];

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { force: true, recursive: true });
  }
});

function fixture(files) {
  const githubDirectory = mkdtempSync(join(tmpdir(), 'anchorage-github-yaml-'));
  temporaryDirectories.push(githubDirectory);
  for (const [path, contents] of Object.entries(files)) {
    const absolutePath = join(githubDirectory, path);
    mkdirSync(dirname(absolutePath), { recursive: true });
    writeFileSync(absolutePath, contents);
  }
  return githubDirectory;
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

function runVerifyGate(step, needs) {
  const variables = Object.keys(step.env ?? {});
  assert.equal(
    variables.length,
    1,
    'the verify gate step carries the needs context in one env variable',
  );
  return spawnSync('bash', ['-e', '-c', step.run], {
    encoding: 'utf8',
    env: { ...process.env, [variables[0]]: JSON.stringify(needs) },
  });
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
  newestWithWarning: npmStub(
    `printf 'npm warn Unknown env config "x"\\n' >&2`,
    `printf '["1.67.0"]\\n'`,
  ),
};

function runNewestVersion(stub) {
  const directory = mkdtempSync(join(tmpdir(), 'anchorage-npm-stub-'));
  temporaryDirectories.push(directory);
  const callLog = join(directory, 'calls');
  writeFileSync(callLog, '');
  writeFileSync(join(directory, 'npm'), stub, { mode: 0o755 });

  const run = spawnSync('bash', ['-e', '-c', newestVersionScript()], {
    encoding: 'utf8',
    env: {
      ...process.env,
      CALL_LOG: callLog,
      PATH: `${directory}:${process.env.PATH}`,
    },
  });

  const calls = readFileSync(callLog, 'utf8').split('\n').filter(Boolean);
  return {
    status: run.status,
    stdout: run.stdout,
    lookups: calls.filter((line) => line.startsWith('npm ')),
    sleeps: calls.filter((line) => line === 'sleep').length,
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

  const directory = mkdtempSync(join(tmpdir(), 'anchorage-step-summary-'));
  temporaryDirectories.push(directory);
  const summaryFile = join(directory, 'summary');
  const run = spawnSync('bash', ['-e', '-c', script], {
    encoding: 'utf8',
    env: { ...process.env, GITHUB_STEP_SUMMARY: summaryFile },
  });
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
          'job `uninstalled` invokes pnpm in `Read the installed version` with no earlier install step',
      },
      {
        file: `${basename(githubDirectory)}/workflows/ci.yml`,
        code: 'MISSING_PNPM_INSTALL',
        message:
          'job `action-setup-without-install` invokes pnpm in `Build` with no earlier install step',
      },
    ],
  );
});

// The checker already parses every tracked workflow, so a job that reaches
// pnpm before installing fails here rather than on the runner.
test('every tracked workflow installs before it invokes pnpm', () => {
  const result = checkGithubYamlFiles(join(repositoryRoot, '.github'));

  assert.deepEqual(
    result.errors.filter(({ code }) => code === 'MISSING_PNPM_INSTALL'),
    [],
  );
});

// The cases below pin the tracked workflows rather than the checker. They live
// here because the checker already parses every .github workflow, and a suite
// of their own would add a verify-core step to run it.
// This one reads the gate's shape and shells out to nothing, so it reports the
// deletions below on every machine.
test('the ci.yml gate job stays reachable and depends on at least one job', () => {
  const { job } = readVerifyGateJob();

  assert.equal(
    job.if,
    'always()',
    'without `if: always()` a failed dependency skips the gate job, and GitHub reports a skipped required check as success',
  );
  assert.ok(
    Array.isArray(job.needs) && job.needs.length > 0,
    'an empty `needs` list leaves the gate passing with nothing verified',
  );
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
    'always()',
    'without `if: always()` a red tripwire suite skips the outcome step',
  );
  assert.deepEqual(
    [...new Set(outcomeReferences(outcome))].sort(),
    ['bundle', 'tests', 'typecheck'],
    'the outcome step reports the outcome of every probe the canary runs',
  );

  for (const [outcomes, succeeds] of [
    [{ typecheck: 'failure', bundle: 'success', tests: 'success' }, false],
    [{ typecheck: 'success', bundle: 'failure', tests: 'success' }, false],
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

    assert.equal(run.lookups.length, scenario.lookups);
    assert.equal(run.sleeps, scenario.sleeps);
    assert.equal(run.stdout, scenario.stdout);
    if (scenario.succeeds) {
      assert.equal(run.status, 0);
    } else {
      assert.ok(run.status > 0, 'the bump step carries the lookup failure');
    }
  });
}

test("the ci.yml canary lookup bounds npm's own fetch retries", () => {
  const run = runNewestVersion(NPM_STUBS.newest);

  assert.equal(run.lookups.length, 1);
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

// The release job spends VISIBILITY_DEADLINE_MS per prerequisite inside
// publish-ordered.mjs, so a budget below that sum kills the job mid-wait and
// leaves a prerequisite published but untagged.
test('the release job budget outlasts every prerequisite visibility wait', () => {
  const release = parse(
    readFileSync(join(repositoryRoot, '.github/workflows/release.yml'), 'utf8'),
  ).jobs.release;

  assert.ok(release, 'the release workflow publishes from the job `release`');
  assert.ok(
    release['timeout-minutes'] * 60_000 >
      PUBLISH_PREREQUISITES.length * VISIBILITY_DEADLINE_MS,
    `timeout-minutes ${release['timeout-minutes']} does not cover ${PUBLISH_PREREQUISITES.length} waits of ${VISIBILITY_DEADLINE_MS}ms`,
  );
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
