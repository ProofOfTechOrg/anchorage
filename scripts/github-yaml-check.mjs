import { lstatSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, extname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isMap, parseDocument, parse as parseYaml } from 'yaml';

import { isInvokedAsEntryPoint } from './entry-point.mjs';
import {
  analyzePnpmCommands,
  classifyPnpmInstallArguments,
  hasPnpmToken,
  isRepositoryRootPath,
} from './shell-command-analysis.mjs';

const FORBIDDEN_CHARACTER =
  // biome-ignore lint/suspicious/noControlCharactersInRegex: YAML excludes these code points from streams.
  /[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F\u0080-\u0084\u0086-\u009F\uFFFE\uFFFF]/u;

const UTF8_DECODER = new TextDecoder('utf-8', { fatal: true });

function toPosix(path) {
  return path.split(sep).join('/');
}

function diagnosticFile(githubDirectory, file) {
  return toPosix(relative(dirname(githubDirectory), file));
}

function fileDiagnostic(githubDirectory, target, code, message) {
  return {
    file: diagnosticFile(githubDirectory, target),
    line: 1,
    column: 1,
    code,
    message,
  };
}

function sourcePosition(source, index) {
  const lines = source.slice(0, index).split(/\r\n|[\r\n]/u);
  return { line: lines.length, column: lines.at(-1).length + 1 };
}

function githubYamlFiles(directory, errors) {
  const files = [];

  function visit(currentDirectory) {
    const entries = readdirSync(currentDirectory, { withFileTypes: true }).sort(
      (left, right) => left.name.localeCompare(right.name),
    );
    for (const entry of entries) {
      const path = join(currentDirectory, entry.name);
      if (entry.isSymbolicLink()) {
        errors.push(
          fileDiagnostic(
            directory,
            path,
            'SYMLINK',
            'symbolic links are not validated; commit the file directly',
          ),
        );
      } else if (entry.isDirectory()) visit(path);
      else if (['.yaml', '.yml'].includes(extname(entry.name)))
        files.push(path);
    }
  }

  visit(directory);
  return files;
}

function isWorkflowFile(githubDirectory, file) {
  return toPosix(relative(githubDirectory, file)).startsWith('workflows/');
}

function actionInstallEntry(entry) {
  if (typeof entry !== 'object' || entry === null || Array.isArray(entry)) {
    return { kind: 'limit', reason: 'unreadable pnpm/action-setup entry' };
  }
  const unknownKey = Object.keys(entry).find(
    (key) => !['args', 'cwd', 'recursive'].includes(key),
  );
  if (unknownKey)
    return {
      kind: 'limit',
      reason: `unmodelled pnpm/action-setup key ${unknownKey}`,
    };
  if (entry.recursive !== undefined && typeof entry.recursive !== 'boolean') {
    return {
      kind: 'limit',
      reason: 'non-literal pnpm/action-setup recursive value',
    };
  }
  if (
    entry.cwd !== undefined &&
    (typeof entry.cwd !== 'string' || entry.cwd.includes('${{'))
  ) {
    return {
      kind: 'limit',
      reason: 'non-literal pnpm/action-setup cwd',
    };
  }
  if (entry.cwd !== undefined && !isRepositoryRootPath(entry.cwd)) {
    return { kind: 'neutral' };
  }
  if (
    entry.args !== undefined &&
    (!Array.isArray(entry.args) ||
      entry.args.some(
        (argument) => typeof argument !== 'string' || argument.includes('${{'),
      ))
  ) {
    return { kind: 'limit', reason: 'non-literal pnpm/action-setup args' };
  }
  const classification = classifyPnpmInstallArguments(entry.args ?? [], {
    recursive: entry.recursive === true,
  });
  if (classification.kind === 'install') return { kind: 'install' };
  if (classification.kind === 'neutral-install') return { kind: 'neutral' };
  return classification;
}

function actionSetupInstall(step) {
  if (
    typeof step?.uses !== 'string' ||
    !step.uses.startsWith('pnpm/action-setup@')
  ) {
    return { kind: 'none' };
  }
  let input = step.with?.run_install;
  if (typeof input === 'string') {
    if (input.includes('${{'))
      return {
        kind: 'limit',
        reason: 'non-literal pnpm/action-setup run_install',
      };
    if (input.trim() === '') return { kind: 'none' };
    try {
      const parsed = parseYaml(input);
      if (typeof parsed === 'string')
        return {
          kind: 'limit',
          reason: 'unreadable pnpm/action-setup run_install',
        };
      input = parsed;
    } catch {
      return {
        kind: 'limit',
        reason: 'unreadable pnpm/action-setup run_install',
      };
    }
  }
  if (input === true) input = { recursive: true };
  if (
    input === undefined ||
    input === null ||
    input === false ||
    (Array.isArray(input) && input.length === 0)
  ) {
    return { kind: 'none' };
  }
  const entries = Array.isArray(input) ? input : [input];
  const results = entries.map(actionInstallEntry);
  if (results.some(({ kind }) => kind === 'install'))
    return { kind: 'install' };
  return (
    results.find(({ kind }) => kind === 'limit') ??
    results.find(({ kind }) => kind === 'neutral') ?? { kind: 'none' }
  );
}

function stepCondition(step) {
  const condition = step?.if;
  if (condition === undefined || condition === null || condition === true) {
    return { kind: 'unconditional' };
  }
  if (
    condition === false ||
    (typeof condition === 'string' &&
      condition.trim().toLowerCase() === 'false')
  ) {
    return { kind: 'never' };
  }
  if (typeof condition !== 'string') return { kind: 'unknown' };
  const normalized = condition.trim();
  if (normalized.toLowerCase() === 'true') return { kind: 'unconditional' };
  if (
    /^\$\{\{\s*github\.(?:event_name|ref|ref_name|repository|workflow)\s*(?:==|!=)\s*(['"])[^'"]+\1\s*\}\}$/u.test(
      normalized,
    )
  ) {
    return { kind: 'stable', key: normalized };
  }
  return { kind: 'unknown' };
}

function continueOnError(step) {
  const value = step?.['continue-on-error'];
  if (value === undefined || value === false) return 'strict';
  if (value === true) return 'tolerated';
  return 'unknown';
}

function effectiveRunSetting(step, job, workflowRunDefaults, setting) {
  return (
    step?.[setting] ??
    job?.defaults?.run?.[setting] ??
    workflowRunDefaults?.[setting]
  );
}

function shellPolicy(shell) {
  if (shell === undefined || shell === null) return { kind: 'standard' };
  const value = String(shell).trim();
  if (['bash', 'sh'].includes(value)) return { kind: 'standard' };
  const firstWord = value.split(/\s+/u, 1)[0];
  const executable = firstWord.split('/').at(-1);
  if (['bash', 'dash', 'sh', 'zsh'].includes(executable)) {
    return { kind: 'custom-bash' };
  }
  return { kind: 'other' };
}

function rootWorkingDirectory(workingDirectory) {
  if (workingDirectory === undefined || workingDirectory === null) return true;
  return isRepositoryRootPath(workingDirectory);
}

function firstPnpmIssue(job, workflowRunDefaults) {
  let unconditionalInstall = false;
  const conditionalInstalls = new Set();
  const steps = Array.isArray(job?.steps) ? job.steps : [];
  for (const [index, step] of steps.entries()) {
    const condition = stepCondition(step);
    if (condition.kind === 'never') continue;
    const matchingConditionalInstall =
      condition.kind === 'stable' && conditionalInstalls.has(condition.key);
    const establishment = unconditionalInstall
      ? 'unconditional'
      : matchingConditionalInstall
        ? 'conditional'
        : 'none';
    const actionInstall = actionSetupInstall(step);
    const failureTolerance = continueOnError(step);
    const canEstablish = failureTolerance === 'strict';
    if (actionInstall.kind === 'limit' && !unconditionalInstall) {
      return {
        index,
        step,
        kind: 'limit',
        command: String(step.uses),
        reason: actionInstall.reason,
      };
    }
    if (
      actionInstall.kind === 'install' &&
      condition.kind === 'unknown' &&
      !unconditionalInstall
    ) {
      return {
        index,
        step,
        kind: 'limit',
        command: String(step.if),
        reason: 'step condition is not stable',
      };
    }
    if (
      actionInstall.kind === 'install' &&
      failureTolerance === 'unknown' &&
      !unconditionalInstall
    ) {
      return {
        index,
        step,
        kind: 'limit',
        command: String(step['continue-on-error']),
        reason: 'continue-on-error is not literal',
      };
    }
    if (actionInstall.kind === 'install' && canEstablish) {
      if (condition.kind === 'unconditional') unconditionalInstall = true;
      else if (condition.kind === 'stable')
        conditionalInstalls.add(condition.key);
    }
    if (typeof step?.run !== 'string') continue;
    const shell = effectiveRunSetting(step, job, workflowRunDefaults, 'shell');
    const shellKind = shellPolicy(shell);
    if (shellKind.kind === 'other') {
      // The Bash grammar cannot classify another shell's strings and comments,
      // so unsupported shells retain a lexical pnpm check.
      if (hasPnpmToken(step.run) && establishment !== 'unconditional') {
        return {
          index,
          step,
          kind: 'limit',
          command: String(shell),
          reason: 'step shell is not Bash-compatible',
        };
      }
      continue;
    }
    const workingDirectory = effectiveRunSetting(
      step,
      job,
      workflowRunDefaults,
      'working-directory',
    );
    const installPolicy =
      shellKind.kind !== 'standard' || !rootWorkingDirectory(workingDirectory)
        ? 'limit'
        : 'establish';
    const analysis = analyzePnpmCommands(step.run, {
      establishment: condition.kind === 'unknown' ? 'none' : establishment,
      installPolicy,
    });
    if (condition.kind === 'unknown' && analysis.usesPnpm) {
      return {
        index,
        step,
        kind: 'limit',
        command: String(step.if),
        reason: 'step condition is not stable',
      };
    }
    const [shellIssue] = analysis.issues;
    if (shellIssue) return { ...shellIssue, index, step };
    if (
      failureTolerance === 'unknown' &&
      analysis.installsPnpm &&
      !unconditionalInstall
    ) {
      return {
        index,
        step,
        kind: 'limit',
        command: String(step['continue-on-error']),
        reason: 'continue-on-error is not literal',
      };
    }
    if (analysis.establishesInstall && canEstablish) {
      if (condition.kind === 'unconditional') unconditionalInstall = true;
      else if (condition.kind === 'stable')
        conditionalInstalls.add(condition.key);
    }
  }
  return undefined;
}

// pnpm resolves a workspace script, and the binaries it runs, out of
// node_modules: a job that reaches one before any step installs fails on the
// runner over the script it was about to run, not over the missing install.
function workflowInstallDiagnostics(githubDirectory, file, workflow) {
  const jobs = workflow?.jobs;
  if (!jobs || typeof jobs !== 'object') return [];
  const diagnostics = [];
  for (const [name, job] of Object.entries(jobs)) {
    const offender = firstPnpmIssue(job, workflow?.defaults?.run);
    if (!offender) continue;
    const step = offender.step.name ?? `step ${offender.index + 1}`;
    if (offender.kind === 'limit') {
      diagnostics.push(
        fileDiagnostic(
          githubDirectory,
          file,
          'PNPM_ANALYSIS_LIMIT',
          `job \`${name}\` step \`${step}\` cannot be shown to run pnpm only after an install (${offender.reason}): ${offender.command}`,
        ),
      );
      continue;
    }
    diagnostics.push(
      fileDiagnostic(
        githubDirectory,
        file,
        'MISSING_PNPM_INSTALL',
        `job \`${name}\` invokes pnpm in \`${step}\` with no earlier install step: ${offender.command}`,
      ),
    );
  }
  return diagnostics;
}

function parserDiagnostic(githubDirectory, file, error) {
  const start = error.linePos?.[0];
  return {
    file: diagnosticFile(githubDirectory, file),
    line: start?.line ?? 1,
    column: start?.col ?? 1,
    code: error.code ?? 'YAML_ERROR',
    message: error.message.replace(/\s+/g, ' ').trim(),
  };
}

export function checkGithubYamlFiles(githubDirectory) {
  const errors = [];
  const warnings = [];

  let rootStat;
  try {
    rootStat = lstatSync(githubDirectory);
  } catch {
    const directory = diagnosticFile(githubDirectory, githubDirectory);
    errors.push(
      fileDiagnostic(
        githubDirectory,
        githubDirectory,
        'MISSING_DIRECTORY',
        `GitHub YAML directory does not exist: ${directory}`,
      ),
    );
    return { filesChecked: 0, errors, warnings };
  }
  if (rootStat.isSymbolicLink()) {
    // The entry-level walk already refuses symlinks; the root needs the same
    // refusal so a `.github -> elsewhere` link can't escape the boundary.
    errors.push(
      fileDiagnostic(
        githubDirectory,
        githubDirectory,
        'SYMLINK',
        'symbolic links are not validated; commit the file directly',
      ),
    );
    return { filesChecked: 0, errors, warnings };
  }
  if (!rootStat.isDirectory()) {
    const directory = diagnosticFile(githubDirectory, githubDirectory);
    errors.push(
      fileDiagnostic(
        githubDirectory,
        githubDirectory,
        'NOT_A_DIRECTORY',
        `GitHub YAML path is not a directory: ${directory}`,
      ),
    );
    return { filesChecked: 0, errors, warnings };
  }

  const files = githubYamlFiles(githubDirectory, errors);

  if (files.length === 0) {
    errors.push(
      fileDiagnostic(
        githubDirectory,
        githubDirectory,
        'NO_YAML_FILES',
        'no GitHub YAML files found',
      ),
    );
  }

  for (const file of files) {
    let source;
    try {
      source = UTF8_DECODER.decode(readFileSync(file));
    } catch {
      errors.push(
        fileDiagnostic(
          githubDirectory,
          file,
          'INVALID_UTF8',
          'file is not valid UTF-8',
        ),
      );
      continue;
    }
    const forbiddenCharacter = FORBIDDEN_CHARACTER.exec(source);
    if (forbiddenCharacter) {
      const position = sourcePosition(source, forbiddenCharacter.index);
      const codePoint = forbiddenCharacter[0]
        .codePointAt(0)
        .toString(16)
        .toUpperCase()
        .padStart(4, '0');
      errors.push({
        file: diagnosticFile(githubDirectory, file),
        ...position,
        code: 'FORBIDDEN_CHARACTER',
        message: `forbidden character U+${codePoint} is not allowed`,
      });
      continue;
    }

    const document = parseDocument(source);
    errors.push(
      ...document.errors.map((error) =>
        parserDiagnostic(githubDirectory, file, error),
      ),
    );
    warnings.push(
      ...document.warnings.map((warning) =>
        parserDiagnostic(githubDirectory, file, warning),
      ),
    );

    let contents;
    try {
      // Materialization surfaces unresolved aliases that parsing alone retains.
      contents = document.toJS();
    } catch (error) {
      errors.push(
        fileDiagnostic(
          githubDirectory,
          file,
          'UNRESOLVED_ALIAS',
          error.message,
        ),
      );
      continue;
    }
    if (!isMap(document.contents)) {
      errors.push(
        fileDiagnostic(
          githubDirectory,
          file,
          'NOT_A_MAPPING',
          'not a YAML mapping',
        ),
      );
      continue;
    }

    // Only workflows declare jobs; .github's other YAML is ISSUE_TEMPLATE.
    if (isWorkflowFile(githubDirectory, file)) {
      errors.push(
        ...workflowInstallDiagnostics(githubDirectory, file, contents),
      );
    }
  }

  return { filesChecked: files.length, errors, warnings };
}

function printDiagnostic(stream, prefix, diagnostic) {
  stream.write(
    `${prefix} ${diagnostic.file}:${diagnostic.line}:${diagnostic.column} ${diagnostic.code}: ${diagnostic.message}\n`,
  );
}

export function runGithubYamlCheck(
  githubDirectory,
  streams = { stderr: process.stderr, stdout: process.stdout },
) {
  const result = checkGithubYamlFiles(githubDirectory);
  for (const warning of result.warnings)
    printDiagnostic(streams.stderr, 'WARN', warning);
  for (const error of result.errors)
    printDiagnostic(streams.stderr, 'FAIL', error);
  if (result.errors.length > 0) {
    return 1;
  }
  streams.stdout.write(
    `GitHub YAML check passed (${result.filesChecked} files).\n`,
  );
  return 0;
}

function main() {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
  // lint-staged appends argv paths, but omissions require checking all of .github.
  process.exitCode = runGithubYamlCheck(join(root, '.github'));
}

if (isInvokedAsEntryPoint(import.meta.url)) {
  main();
}
