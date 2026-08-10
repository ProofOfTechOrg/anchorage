#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import {
  basename,
  dirname,
  extname,
  isAbsolute,
  join,
  relative,
  resolve,
  sep,
} from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import GithubSlugger from 'github-slugger';
import { toString as markdownText } from 'mdast-util-to-string';
import remarkGfm from 'remark-gfm';
import remarkParse from 'remark-parse';
import { unified } from 'unified';
import { visit } from 'unist-util-visit';

const IGNORED_DIRECTORIES = new Set([
  '.git',
  '.wrangler',
  'dist',
  'node_modules',
]);
const EXTERNAL_WARNING_STATUSES = new Set([401, 403, 429]);
const EXTERNAL_FAILURE_STATUSES = new Set([404, 410]);
const INTERNAL_MILESTONE_PATTERN =
  /\b(?:CI-M-\d{3}(?:-\d{3})?|DL-\d{3}|INV-\d+|M-\d{3}|RA-\d{3}|[A-Z]-S\d+|R-[A-Z0-9][A-Z0-9-]*|[A-Z]-D\d+|D(?:[2-9]|\d{2,})|F\d+|P\d+(?:-lite)?|Track [A-Z]|Phase \d+)\b/g;
const VOLATILE_COUNT_PATTERN =
  /(?<![\w.-])\d[\d,]*(?![.\d])\s+(?:tests?|test files?|packages?)\b/gi;
const REQUIRED_PUBLIC_URLS = [
  'https://anchorage.proofoftech.org/',
  'https://github.com/ProofOfTechOrg/anchorage',
  'https://www.npmjs.com/package/@proofoftech/breakwater',
  'https://www.npmjs.com/package/@proofoftech/flowsafe',
  'https://proofoftechorg.github.io/anchorage/',
];
const MARKDOWN_PARSER = unified().use(remarkParse).use(remarkGfm);

function toPosix(path) {
  return path.split(sep).join('/');
}

function lineNumberAt(text, index) {
  let line = 1;
  for (let cursor = 0; cursor < index; cursor += 1) {
    if (text.charCodeAt(cursor) === 10) line += 1;
  }
  return line;
}

function parseMarkdown(markdown) {
  return MARKDOWN_PARSER.parse(markdown);
}

function htmlCodePoint(digits, radix) {
  const codePoint = Number.parseInt(digits, radix);
  if (
    codePoint === 0 ||
    codePoint > 0x10ffff ||
    (codePoint >= 0xd800 && codePoint <= 0xdfff)
  ) {
    return '\uFFFD';
  }
  return String.fromCodePoint(codePoint);
}

function decodeRawHtmlAttribute(value) {
  return value
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'")
    .replace(/&#x([0-9a-f]+);/gi, (_, digits) => htmlCodePoint(digits, 16))
    .replace(/&#([0-9]+);/g, (_, digits) => htmlCodePoint(digits, 10));
}

function rawHtmlAttributes(markdown, node, names) {
  if (node.value.trimStart().startsWith('<!--')) return [];

  const attributes = [];
  const pattern = /\b([a-z][\w:-]*)\s*=\s*(["'])([\s\S]*?)\2/gi;
  for (const match of node.value.matchAll(pattern)) {
    const name = match[1].toLowerCase();
    if (!names.has(name)) continue;
    attributes.push({
      name,
      value: decodeRawHtmlAttribute(match[3].trim()),
      line: lineNumberAt(
        markdown,
        (node.position?.start.offset ?? 0) + match.index,
      ),
    });
  }
  return attributes;
}

export function collectMarkdownAnchors(markdown) {
  const anchors = new Set();
  const slugger = new GithubSlugger();

  visit(parseMarkdown(markdown), (node) => {
    if (node.type === 'heading') {
      const slug = slugger.slug(markdownText(node, { includeHtml: false }));
      if (slug) anchors.add(slug);
      return;
    }
    if (node.type === 'html') {
      for (const attribute of rawHtmlAttributes(
        markdown,
        node,
        new Set(['id', 'name']),
      )) {
        anchors.add(safeDecode(attribute.value));
      }
    }
  });

  return anchors;
}

function markdownPolicySource(markdown) {
  const characters = markdown.split('');
  visit(parseMarkdown(markdown), (node) => {
    const isComment =
      node.type === 'html' && node.value.trimStart().startsWith('<!--');
    if (!['code', 'inlineCode'].includes(node.type) && !isComment) return;

    const start = node.position?.start.offset;
    const end = node.position?.end.offset;
    if (start === undefined || end === undefined) return;
    for (let index = start; index < end; index += 1) {
      if (characters[index] !== '\n') characters[index] = ' ';
    }
  });
  return characters.join('');
}

function markdownDefinitions(tree) {
  const definitions = new Map();
  visit(tree, 'definition', (node) => {
    definitions.set(node.identifier, node.url);
  });
  return definitions;
}

function referencedTarget(node, definitions) {
  if (node.type === 'link' || node.type === 'image') return node.url;
  if (node.type === 'linkReference' || node.type === 'imageReference') {
    return definitions.get(node.identifier);
  }
  return undefined;
}

export function collectMarkdownLinks(markdown) {
  const tree = parseMarkdown(markdown);
  const definitions = markdownDefinitions(tree);
  const links = [];
  visit(tree, (node) => {
    const target = referencedTarget(node, definitions);
    if (target !== undefined) {
      links.push({
        target,
        line: node.position?.start.line ?? 1,
      });
      return;
    }
    if (node.type === 'html') {
      for (const attribute of rawHtmlAttributes(
        markdown,
        node,
        new Set(['href', 'src']),
      )) {
        links.push({ target: attribute.value, line: attribute.line });
      }
    }
  });

  return links;
}

function walk(directory, predicate) {
  const files = [];
  if (!existsSync(directory)) return files;

  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (entry.isDirectory() && IGNORED_DIRECTORIES.has(entry.name)) continue;
    const path = join(directory, entry.name);
    if (entry.isDirectory() && toPosix(path).endsWith('/docs/api')) {
      continue;
    }
    if (entry.isDirectory()) files.push(...walk(path, predicate));
    else if (entry.isFile() && predicate(path)) files.push(path);
  }
  return files;
}

function repositoryMarkdownFiles(root) {
  const result = spawnSync(
    'git',
    [
      'ls-files',
      '-z',
      '--cached',
      '--others',
      '--exclude-standard',
      '--',
      '*.md',
    ],
    { cwd: root, encoding: 'utf8' },
  );
  if (result.status === 0) {
    return result.stdout
      .split('\0')
      .filter(Boolean)
      .map((file) => resolve(root, file))
      .filter(existsSync)
      .filter((file) => basename(file).toLowerCase() !== 'claude.md')
      .sort();
  }
  return walk(root, (file) => extname(file).toLowerCase() === '.md').sort();
}

function isExternal(target) {
  return /^https?:\/\//i.test(target);
}

function isIgnoredScheme(target) {
  return /^(?:data|mailto|tel):/i.test(target);
}

function safeDecode(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function splitLocalTarget(target) {
  const hash = target.indexOf('#');
  const beforeHash = hash === -1 ? target : target.slice(0, hash);
  const query = beforeHash.indexOf('?');
  return {
    path: safeDecode(query === -1 ? beforeHash : beforeHash.slice(0, query)),
    anchor: hash === -1 ? undefined : safeDecode(target.slice(hash + 1)),
  };
}

function resolveLocalTarget(root, sourceFile, target) {
  const split = splitLocalTarget(target);
  const candidate = split.path
    ? resolve(dirname(sourceFile), split.path)
    : sourceFile;
  const relativeToRoot = relative(root, candidate);
  if (
    isAbsolute(relativeToRoot) ||
    relativeToRoot === '..' ||
    relativeToRoot.startsWith(`..${sep}`)
  ) {
    return { ...split, outsideRoot: true, path: candidate };
  }

  if (!existsSync(candidate)) return { ...split, path: candidate };
  const stats = statSync(candidate);
  if (!stats.isDirectory()) return { ...split, path: candidate };

  const readme = join(candidate, 'README.md');
  return {
    ...split,
    path: existsSync(readme) ? readme : candidate,
    directoryWithoutReadme: !existsSync(readme),
  };
}

function manifestFileIncludes(manifest, packageRelativePath) {
  if (
    packageRelativePath === 'package.json' ||
    /^(?:README|CHANGELOG|LICEN[CS]E)(?:\.[^/]*)?$/i.test(packageRelativePath)
  ) {
    return true;
  }

  return (manifest.files ?? []).some((entry) => {
    const normalized = entry.replace(/^\.?\//, '').replace(/\/$/, '');
    return (
      packageRelativePath === normalized ||
      packageRelativePath.startsWith(`${normalized}/`)
    );
  });
}

function shippedPackageContext(sourceFile, manifests) {
  for (const [packageRoot, manifest] of manifests) {
    const sourceRelative = toPosix(relative(packageRoot, sourceFile));
    if (
      !sourceRelative.startsWith('../') &&
      manifestFileIncludes(manifest, sourceRelative)
    ) {
      return { packageRoot, manifest };
    }
  }
  return undefined;
}

function loadPackageManifests(root) {
  const manifests = new Map();
  const packagesRoot = resolve(root, 'packages');
  if (!existsSync(packagesRoot)) return manifests;

  for (const entry of readdirSync(packagesRoot, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const packageRoot = resolve(packagesRoot, entry.name);
    const manifestPath = join(packageRoot, 'package.json');
    if (!existsSync(manifestPath)) continue;
    const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
    if (manifest.private || !manifest.name) continue;
    manifests.set(packageRoot, manifest);
  }
  return manifests;
}

function diagnostic(root, file, line, message) {
  return {
    file: toPosix(relative(root, file)),
    line,
    message,
  };
}

function checkLocalLinks(root, markdownFiles, manifests) {
  const errors = [];
  const anchors = new Map();

  const anchorsFor = (file) => {
    let result = anchors.get(file);
    if (!result) {
      result = collectMarkdownAnchors(readFileSync(file, 'utf8'));
      anchors.set(file, result);
    }
    return result;
  };

  for (const sourceFile of markdownFiles) {
    const markdown = readFileSync(sourceFile, 'utf8');
    const packageContext = shippedPackageContext(sourceFile, manifests);

    for (const link of collectMarkdownLinks(markdown)) {
      if (!link.target) continue;
      if (/^javascript:/i.test(link.target)) {
        errors.push(
          diagnostic(
            root,
            sourceFile,
            link.line,
            `unsafe URL scheme: ${link.target}`,
          ),
        );
        continue;
      }
      if (isIgnoredScheme(link.target)) {
        continue;
      }
      if (isExternal(link.target)) {
        try {
          new URL(link.target);
        } catch {
          errors.push(
            diagnostic(
              root,
              sourceFile,
              link.line,
              `external URL is invalid: ${link.target}`,
            ),
          );
        }
        continue;
      }
      if (link.target.startsWith('/')) {
        errors.push(
          diagnostic(
            root,
            sourceFile,
            link.line,
            `repository-local link must be relative: ${link.target}`,
          ),
        );
        continue;
      }

      const resolved = resolveLocalTarget(root, sourceFile, link.target);
      if (resolved.outsideRoot) {
        errors.push(
          diagnostic(
            root,
            sourceFile,
            link.line,
            `link escapes the repository: ${link.target}`,
          ),
        );
        continue;
      }
      if (!existsSync(resolved.path)) {
        errors.push(
          diagnostic(
            root,
            sourceFile,
            link.line,
            `link target does not exist: ${link.target}`,
          ),
        );
        continue;
      }
      if (resolved.directoryWithoutReadme) {
        errors.push(
          diagnostic(
            root,
            sourceFile,
            link.line,
            `linked directory has no README.md: ${link.target}`,
          ),
        );
        continue;
      }

      const targetRelative = toPosix(relative(root, resolved.path));
      if (
        basename(sourceFile).toLowerCase() !== 'claude.md' &&
        (basename(resolved.path).toLowerCase() === 'claude.md' ||
          targetRelative.split('/').includes('.notes') ||
          (!targetRelative.startsWith('docs/proposals/') &&
            /(?:^|[-_])(?:plan|roadmap)(?:[-_.]|$)/i.test(
              basename(resolved.path),
            )))
      ) {
        errors.push(
          diagnostic(
            root,
            sourceFile,
            link.line,
            `public documentation links to an internal file: ${link.target}`,
          ),
        );
      }

      if (packageContext) {
        const packageRelative = toPosix(
          relative(packageContext.packageRoot, resolved.path),
        );
        if (
          packageRelative.startsWith('../') ||
          !manifestFileIncludes(packageContext.manifest, packageRelative)
        ) {
          errors.push(
            diagnostic(
              root,
              sourceFile,
              link.line,
              `link leaves the published package; use a permanent GitHub URL: ${link.target}`,
            ),
          );
        }
      }

      if (
        resolved.anchor &&
        extname(resolved.path).toLowerCase() === '.md' &&
        !anchorsFor(resolved.path).has(resolved.anchor)
      ) {
        errors.push(
          diagnostic(
            root,
            sourceFile,
            link.line,
            `Markdown anchor does not exist: #${resolved.anchor}`,
          ),
        );
      }
    }
  }

  return errors;
}

function checkPublicContentPolicy(root, markdownFiles) {
  const errors = [];

  for (const file of markdownFiles) {
    const relativePath = toPosix(relative(root, file));
    if (
      basename(file).toLowerCase() === 'changelog.md' ||
      relativePath.startsWith('docs/proposals/')
    ) {
      continue;
    }

    const markdown = markdownPolicySource(readFileSync(file, 'utf8'));
    for (const match of markdown.matchAll(INTERNAL_MILESTONE_PATTERN)) {
      errors.push(
        diagnostic(
          root,
          file,
          lineNumberAt(markdown, match.index),
          `internal milestone token is not public documentation: ${match[0]}`,
        ),
      );
    }
    for (const match of markdown.matchAll(VOLATILE_COUNT_PATTERN)) {
      errors.push(
        diagnostic(
          root,
          file,
          lineNumberAt(markdown, match.index),
          `volatile hard-coded count is not allowed: ${match[0]}`,
        ),
      );
    }
  }

  return errors;
}

function checkCanonicalUrls(root) {
  const manifestPath = resolve(root, 'package.json');
  const readmePath = resolve(root, 'README.md');
  if (!existsSync(manifestPath) || !existsSync(readmePath)) return [];

  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  if (manifest.name !== 'anchorage') return [];
  const readme = readFileSync(readmePath, 'utf8');

  return REQUIRED_PUBLIC_URLS.filter((url) => !readme.includes(url)).map(
    (url) =>
      diagnostic(
        root,
        readmePath,
        1,
        `README is missing the canonical public URL: ${url}`,
      ),
  );
}

function exportedStrings(value) {
  if (typeof value === 'string') return [value];
  if (Array.isArray(value)) return value.flatMap(exportedStrings);
  if (value && typeof value === 'object') {
    return Object.values(value).flatMap(exportedStrings);
  }
  return [];
}

function checkReadmeExportCoverage(root, manifests) {
  const errors = [];

  for (const [packageRoot, manifest] of manifests) {
    if (manifest.private || !manifest.exports || !manifest.name) continue;
    const readmePath = join(packageRoot, 'README.md');
    if (!existsSync(readmePath)) {
      errors.push(diagnostic(root, packageRoot, 1, 'package has no README.md'));
      continue;
    }
    const readme = readFileSync(readmePath, 'utf8');

    for (const exportName of Object.keys(manifest.exports)) {
      if (exportName === './package.json') continue;
      const publicName =
        exportName === '.'
          ? manifest.name
          : `${manifest.name}/${exportName.replace(/^\.\//, '')}`;
      if (!readme.includes(publicName)) {
        errors.push(
          diagnostic(
            root,
            readmePath,
            1,
            `README does not name public export ${publicName}`,
          ),
        );
      }
    }
  }

  return errors;
}

function checkManifestClaims(root, manifests) {
  const errors = [];

  for (const [packageRoot, manifest] of manifests) {
    const readmePath = join(packageRoot, 'README.md');
    if (!existsSync(readmePath)) continue;
    const readme = readFileSync(readmePath, 'utf8');
    const nodeRange = manifest.engines?.node;

    if (
      typeof nodeRange === 'string' &&
      (!readme.includes('Node') || !readme.includes(nodeRange))
    ) {
      errors.push(
        diagnostic(
          root,
          readmePath,
          1,
          `README does not state manifest Node engine ${nodeRange}`,
        ),
      );
    }

    for (const [peer, range] of Object.entries(
      manifest.peerDependencies ?? {},
    )) {
      if (
        typeof range === 'string' &&
        (!readme.includes(peer) || !readme.includes(range))
      ) {
        errors.push(
          diagnostic(
            root,
            readmePath,
            1,
            `README does not state manifest peer range ${peer} ${range}`,
          ),
        );
      }
    }
  }

  return errors;
}

function sourcePathForExport(packageRoot, exportedValue) {
  const targets = exportedStrings(exportedValue);
  const target =
    targets.find((value) => /^\.\/dist\/.*\.js$/.test(value)) ??
    targets.find((value) => /^\.\/dist\//.test(value));
  if (!target) return undefined;
  return resolve(
    packageRoot,
    target.replace(/^\.\/dist\//, 'src/').replace(/\.(?:mjs|cjs|js)$/, '.ts'),
  );
}

function collectTypeDocEntryPoints(manifests) {
  const entries = new Set();
  const configs = [...manifests.keys()].flatMap((packageRoot) =>
    walk(packageRoot, (file) => basename(file) === 'typedoc.json'),
  );

  for (const configPath of configs) {
    const config = JSON.parse(readFileSync(configPath, 'utf8'));
    for (const entry of config.entryPoints ?? []) {
      entries.add(resolve(dirname(configPath), entry));
    }
  }
  return entries;
}

function checkTypeDocCoverage(root, manifests) {
  const errors = [];
  const documented = collectTypeDocEntryPoints(manifests);

  for (const [packageRoot, manifest] of manifests) {
    for (const [exportName, exportedValue] of Object.entries(
      manifest.exports ?? {},
    )) {
      if (exportName === './package.json') continue;
      const sourcePath = sourcePathForExport(packageRoot, exportedValue);
      if (!sourcePath) {
        errors.push(
          diagnostic(
            root,
            join(packageRoot, 'package.json'),
            1,
            `cannot map export ${exportName} to a TypeScript entry point`,
          ),
        );
      } else if (!documented.has(sourcePath)) {
        errors.push(
          diagnostic(
            root,
            join(packageRoot, 'typedoc.json'),
            1,
            `TypeDoc does not cover export ${exportName} (${toPosix(
              relative(packageRoot, sourcePath),
            )})`,
          ),
        );
      }
    }
  }

  return errors;
}

function markdownGraph(root, markdownFiles) {
  const known = new Set(markdownFiles);
  const graph = new Map();
  for (const sourceFile of markdownFiles) {
    const targets = new Set();
    for (const link of collectMarkdownLinks(readFileSync(sourceFile, 'utf8'))) {
      if (
        !link.target ||
        isExternal(link.target) ||
        isIgnoredScheme(link.target)
      ) {
        continue;
      }
      const resolved = resolveLocalTarget(root, sourceFile, link.target);
      if (
        !resolved.outsideRoot &&
        existsSync(resolved.path) &&
        extname(resolved.path).toLowerCase() === '.md' &&
        known.has(resolved.path)
      ) {
        targets.add(resolved.path);
      }
    }
    graph.set(sourceFile, targets);
  }
  return graph;
}

function checkOrphanedGuides(root, markdownFiles) {
  const docsIndex = resolve(root, 'docs/README.md');
  if (!existsSync(docsIndex)) return [];

  const graph = markdownGraph(root, markdownFiles);
  const seeds = [
    'README.md',
    'CONTRIBUTING.md',
    'SECURITY.md',
    'SUPPORT.md',
    'docs/README.md',
    'packages/breakwater/README.md',
    'packages/flowsafe/README.md',
  ]
    .map((file) => resolve(root, file))
    .filter((file) => graph.has(file));
  const reachable = new Set();
  const pending = [...seeds];
  while (pending.length > 0) {
    const file = pending.pop();
    if (reachable.has(file)) continue;
    reachable.add(file);
    for (const target of graph.get(file) ?? []) pending.push(target);
  }

  return markdownFiles
    .filter((file) => {
      const relativePath = toPosix(relative(root, file));
      return (
        relativePath.startsWith('docs/') &&
        basename(file).toLowerCase() !== 'claude.md' &&
        !reachable.has(file)
      );
    })
    .map((file) =>
      diagnostic(
        root,
        file,
        1,
        'guide is not reachable from a public documentation index',
      ),
    );
}

export function checkRepository({
  root,
  markdownFiles = repositoryMarkdownFiles(root),
  packageChecks = true,
  orphanChecks = true,
}) {
  const manifests = packageChecks ? loadPackageManifests(root) : new Map();
  const errors = [
    ...checkLocalLinks(root, markdownFiles, manifests),
    ...checkPublicContentPolicy(root, markdownFiles),
    ...checkCanonicalUrls(root),
    ...(packageChecks ? checkReadmeExportCoverage(root, manifests) : []),
    ...(packageChecks ? checkManifestClaims(root, manifests) : []),
    ...(packageChecks ? checkTypeDocCoverage(root, manifests) : []),
    ...(orphanChecks ? checkOrphanedGuides(root, markdownFiles) : []),
  ];

  return {
    filesChecked: markdownFiles.length,
    errors: errors.sort(
      (left, right) =>
        left.file.localeCompare(right.file) ||
        left.line - right.line ||
        left.message.localeCompare(right.message),
    ),
  };
}

export function collectExternalUrls(markdownFiles) {
  const urls = new Set();
  for (const file of markdownFiles) {
    const markdown = readFileSync(file, 'utf8');
    for (const link of collectMarkdownLinks(markdown)) {
      if (isExternal(link.target)) urls.add(link.target);
    }
    for (const match of markdownPolicySource(markdown).matchAll(
      /https?:\/\/[^\s<>"'`\])]+/g,
    )) {
      urls.add(match[0].replace(/[),.;:]+$/, ''));
    }
  }
  return [...urls]
    .flatMap((url) => {
      try {
        const parsed = new URL(url);
        parsed.hash = '';
        return [parsed.href];
      } catch {
        return [];
      }
    })
    .filter((url) => {
      const hostname = new URL(url).hostname;
      return !['127.0.0.1', 'localhost'].includes(hostname);
    })
    .sort();
}

async function requestExternal(url, method, fetchImpl, timeoutMs) {
  return fetchImpl(url, {
    method,
    redirect: 'follow',
    headers: {
      'user-agent': 'anchorage-docs-link-check/1.0',
      ...(method === 'GET' ? { range: 'bytes=0-0' } : {}),
    },
    signal: AbortSignal.timeout(timeoutMs),
  });
}

export async function checkExternalUrl(
  url,
  { fetchImpl = fetch, retries = 2, timeoutMs = 15_000 } = {},
) {
  let lastFailure;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      let response = await requestExternal(url, 'HEAD', fetchImpl, timeoutMs);
      if ([405, 501].includes(response.status)) {
        response = await requestExternal(url, 'GET', fetchImpl, timeoutMs);
      }

      if (response.status >= 200 && response.status < 400) {
        return { kind: 'ok', url, status: response.status };
      }
      if (EXTERNAL_WARNING_STATUSES.has(response.status)) {
        return { kind: 'warning', url, status: response.status };
      }
      if (EXTERNAL_FAILURE_STATUSES.has(response.status)) {
        return { kind: 'error', url, status: response.status };
      }

      lastFailure = { kind: 'error', url, status: response.status };
      if (response.status < 500) return lastFailure;
    } catch (error) {
      lastFailure = {
        kind: 'error',
        url,
        message: error instanceof Error ? error.message : String(error),
      };
    }
  }

  return lastFailure;
}

export async function checkExternalUrls(
  urls,
  { concurrency = 6, ...options } = {},
) {
  const results = new Array(urls.length);
  let next = 0;

  async function worker() {
    while (next < urls.length) {
      const index = next;
      next += 1;
      results[index] = await checkExternalUrl(urls[index], options);
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(concurrency, Math.max(urls.length, 1)) },
      worker,
    ),
  );
  return results;
}

function printDiagnostics(errors) {
  for (const error of errors) {
    process.stderr.write(`${error.file}:${error.line}: ${error.message}\n`);
  }
}

async function main() {
  const flags = new Set(process.argv.slice(2));
  for (const flag of flags) {
    if (flag !== '--external') throw new Error(`Unknown option: ${flag}`);
  }

  const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
  const markdownFiles = repositoryMarkdownFiles(root);
  const result = checkRepository({ root, markdownFiles });
  if (result.errors.length > 0) {
    process.stderr.write(
      `Documentation check failed with ${result.errors.length} error(s):\n`,
    );
    printDiagnostics(result.errors);
    process.exitCode = 1;
    return;
  }

  process.stdout.write(
    `Documentation check passed (${result.filesChecked} Markdown files).\n`,
  );
  if (!flags.has('--external')) return;

  const urls = collectExternalUrls(markdownFiles);
  const linkResults = await checkExternalUrls(urls);
  const warnings = linkResults.filter((entry) => entry.kind === 'warning');
  const failures = linkResults.filter((entry) => entry.kind === 'error');

  for (const warning of warnings) {
    process.stderr.write(
      `External link indeterminate (${warning.status}): ${warning.url}\n`,
    );
  }
  for (const failure of failures) {
    const detail =
      failure.status === undefined ? failure.message : failure.status;
    process.stderr.write(`External link failed (${detail}): ${failure.url}\n`);
  }

  if (failures.length > 0) process.exitCode = 1;
  else {
    process.stdout.write(
      `External link check passed (${urls.length} URLs, ${warnings.length} indeterminate).\n`,
    );
  }
}

const invokedPath = process.argv[1]
  ? pathToFileURL(resolve(process.argv[1])).href
  : undefined;
if (invokedPath === import.meta.url) {
  await main();
}
