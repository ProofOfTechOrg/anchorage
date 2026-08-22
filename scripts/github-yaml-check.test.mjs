import assert from 'node:assert/strict';
import {
  mkdirSync,
  mkdtempSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { afterEach, test } from 'node:test';
import {
  checkGithubYamlFiles,
  runGithubYamlCheck,
} from './github-yaml-check.mjs';

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
