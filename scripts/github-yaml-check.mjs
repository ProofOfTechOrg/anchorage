import { lstatSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, extname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { isMap, parseDocument } from 'yaml';

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

    try {
      // Materialization surfaces unresolved aliases that parsing alone retains.
      document.toJS();
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

const invokedPath = process.argv[1]
  ? pathToFileURL(resolve(process.argv[1])).href
  : undefined;
if (invokedPath === import.meta.url) {
  main();
}
