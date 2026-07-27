import { spawnSync } from 'node:child_process';

const REACT_DOCTOR_COMMIT = 'e8939ca38358a24884ce7198e9ad1e4b7c025170';
const REACT_DOCTOR_TARGET = `github:gcharang/react-doctor#${REACT_DOCTOR_COMMIT}`;
const REACT_DOCTOR_BUILD = `react-doctor-pinned@https://codeload.github.com/gcharang/react-doctor/tar.gz/${REACT_DOCTOR_COMMIT}`;

const [scope = 'full', base] = process.argv.slice(2);
if (scope !== 'full' && scope !== 'changed') {
  throw new Error(`Unsupported React Doctor scope: ${scope}`);
}
if (scope === 'changed' && !base) {
  throw new Error('The changed scope requires a base revision.');
}

const args = [
  'dlx',
  `--allow-build=${REACT_DOCTOR_BUILD}`,
  REACT_DOCTOR_TARGET,
  '--scope',
  scope,
  '--verbose',
  '--blocking',
  'warning',
  '--json',
];
if (base) args.push('--base', base);

const result = spawnSync('pnpm', args, {
  encoding: 'utf8',
  maxBuffer: 64 * 1024 * 1024,
});

if (result.error) throw result.error;
if (result.stderr) process.stderr.write(result.stderr);

const schemaMarker = result.stdout.indexOf('"schemaVersion"');
const jsonStart =
  schemaMarker === -1 ? -1 : result.stdout.lastIndexOf('{', schemaMarker);
if (jsonStart === -1) {
  process.stdout.write(result.stdout);
  throw new Error('React Doctor did not emit its JSON report.');
}

const installOutput = result.stdout.slice(0, jsonStart);
if (installOutput) process.stdout.write(installOutput);

const report = JSON.parse(result.stdout.slice(jsonStart));
for (const diagnostic of report.diagnostics ?? []) {
  const location =
    typeof diagnostic.line === 'number'
      ? `${diagnostic.filePath}:${diagnostic.line}`
      : diagnostic.filePath;
  console.error(
    `${diagnostic.severity.toUpperCase()} ${location} ${diagnostic.rule}: ${diagnostic.title}`,
  );
}

const projects = Array.isArray(report.projects) ? report.projects : [];
const skipped = projects.flatMap((project) => project.skippedChecks ?? []);
const incompleteProjects = projects.filter(
  (project) => project.complete !== true,
);
const missingFullProject = scope === 'full' && projects.length === 0;
const emptyChangedScan =
  scope === 'changed' &&
  report.reactDetected !== true &&
  report.error == null &&
  projects.length === 0;
const summary = report.summary ?? {};

console.log(
  `React Doctor: ${summary.errorCount ?? 0} errors, ${summary.warningCount ?? 0} warnings, ` +
    `${projects.reduce((count, project) => count + (project.analyzedFileCount ?? 0), 0)} files`,
);

if (
  (!emptyChangedScan && report.reactDetected !== true) ||
  report.error != null ||
  missingFullProject ||
  incompleteProjects.length > 0 ||
  skipped.length > 0
) {
  const reasons = [
    report.reactDetected === true || emptyChangedScan
      ? undefined
      : 'React was not detected',
    report.error == null ? undefined : String(report.error),
    missingFullProject ? 'no project was analyzed' : undefined,
    incompleteProjects.length > 0
      ? `${incompleteProjects.length} project scan(s) were incomplete`
      : undefined,
    skipped.length > 0
      ? `skipped checks: ${[...new Set(skipped)].join(', ')}`
      : undefined,
  ].filter(Boolean);
  throw new Error(`React Doctor report is incomplete: ${reasons.join('; ')}`);
}

if (result.status !== 0) process.exit(result.status ?? 1);
