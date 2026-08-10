import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { afterEach, test } from 'node:test';
import {
  checkExternalUrl,
  checkRepository,
  collectExternalUrls,
  collectMarkdownAnchors,
  collectMarkdownLinks,
} from './docs-check.mjs';

const temporaryDirectories = [];

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { force: true, recursive: true });
  }
});

function fixture(files) {
  const root = mkdtempSync(join(tmpdir(), 'anchorage-docs-check-'));
  temporaryDirectories.push(root);
  for (const [path, contents] of Object.entries(files)) {
    const absolutePath = join(root, path);
    mkdirSync(dirname(absolutePath), { recursive: true });
    writeFileSync(absolutePath, contents);
  }
  return root;
}

function markdownFiles(root, paths) {
  return paths.map((path) => join(root, path));
}

test('GitHub-compatible anchors include duplicate suffixes and explicit ids', () => {
  const anchors = collectMarkdownAnchors(`
# Public API
## Public API
Setext heading
--------------
<span id="stable-anchor"></span>
`);

  assert.deepEqual(
    [...anchors],
    ['public-api', 'public-api-1', 'setext-heading', 'stable-anchor'],
  );
});

test('heading anchors use rendered punctuation, entities, and Unicode text', () => {
  const anchors =
    collectMarkdownAnchors(`# API: \`create()\` &amp; [résumé](guide.md)
# API: \`create()\` &amp; [résumé](guide.md)
## Привет мир 👋
# Hello <span>world</span>
`);

  assert.deepEqual(
    [...anchors],
    [
      'api-create--résumé',
      'api-create--résumé-1',
      'привет-мир-',
      'hello-world',
    ],
  );
});

test('link extraction ignores examples in code fences and inline code', () => {
  const links = collectMarkdownLinks(`
[real](guide.md)

\`\`\`md
[ignored](missing.md)
\`\`\`

\`[also ignored](missing.md)\`

<!--
[commented out](missing-from-comment.md)
-->
`);

  assert.deepEqual(links, [{ target: 'guide.md', line: 2 }]);
});

test('reference links, images, and quoted raw HTML attributes resolve', () => {
  const links = collectMarkdownLinks(
    [
      '[Guide][guide]',
      '![Diagram][diagram]',
      '',
      '[guide]: <guide&amp;one.md>',
      '[diagram]: assets/diagram.png',
      '',
      "<a href='raw&amp;guide.md'>Raw</a>",
      '<img src="assets/raw.png">',
      '<!-- <a href="ignored.md"> -->',
    ].join('\n'),
  );

  assert.deepEqual(links, [
    { target: 'guide&one.md', line: 1 },
    { target: 'assets/diagram.png', line: 2 },
    { target: 'raw&guide.md', line: 7 },
    { target: 'assets/raw.png', line: 8 },
  ]);
});

test('malformed Markdown does not create links or headings', () => {
  assert.deepEqual(collectMarkdownLinks('[broken](missing.md'), []);
  assert.deepEqual([...collectMarkdownAnchors('#Not a heading')], []);
  assert.deepEqual(
    collectMarkdownLinks('<!-- [hidden](missing.md)\nstill commented'),
    [],
  );
  assert.deepEqual(
    collectMarkdownLinks('<a href="&#x110000;">invalid entity</a>'),
    [{ target: '\uFFFD', line: 1 }],
  );
});

test('shorter fences cannot close longer Markdown code blocks', () => {
  const markdown = `
\`\`\`\`md
# Hidden heading
[hidden](missing.md)
\`\`\`
# Still hidden
[also hidden](missing-too.md)
\`\`\`\`
# Visible heading
[visible](guide.md)
`;

  assert.deepEqual([...collectMarkdownAnchors(markdown)], ['visible-heading']);
  assert.deepEqual(collectMarkdownLinks(markdown), [
    { target: 'guide.md', line: 10 },
  ]);
});

test('local files, directory READMEs, and Markdown anchors pass', () => {
  const root = fixture({
    'README.md': `# Start

[Guide](docs/guide.md#target-heading)
[Docs](docs/)
[Self](#start)
`,
    'docs/README.md': '# Documentation\n',
    'docs/guide.md': '# Target heading\n',
  });

  const result = checkRepository({
    root,
    markdownFiles: markdownFiles(root, [
      'README.md',
      'docs/README.md',
      'docs/guide.md',
    ]),
    packageChecks: false,
    orphanChecks: false,
  });

  assert.deepEqual(result.errors, []);
});

test('missing files, directory READMEs, and Markdown anchors fail', () => {
  const root = fixture({
    'README.md': `# Start

[Missing](missing.md)
[No index](empty/)
[Bad anchor](guide.md#absent)
`,
    'empty/placeholder.txt': '',
    'guide.md': '# Present\n',
  });

  const result = checkRepository({
    root,
    markdownFiles: markdownFiles(root, ['README.md', 'guide.md']),
    packageChecks: false,
    orphanChecks: false,
  });

  assert.deepEqual(
    result.errors.map((error) => error.message),
    [
      'link target does not exist: missing.md',
      'linked directory has no README.md: empty/',
      'Markdown anchor does not exist: #absent',
    ],
  );
});

test('invalid and unsafe URLs fail before the scheduled network check', () => {
  const root = fixture({
    'README.md': `[Invalid](https://[invalid)
[Unsafe](javascript:alert)
`,
  });

  const result = checkRepository({
    root,
    markdownFiles: markdownFiles(root, ['README.md']),
    packageChecks: false,
    orphanChecks: false,
  });

  assert.deepEqual(result.errors, [
    {
      file: 'README.md',
      line: 1,
      message: 'external URL is invalid: https://[invalid',
    },
    {
      file: 'README.md',
      line: 2,
      message: 'unsafe URL scheme: javascript:alert',
    },
  ]);
});

test('public guides reject internal milestone tokens and volatile counts', () => {
  const root = fixture({
    'README.md': `# Public guide

This describes DL-001, INV-3, D4, F4, and P7, and claims 978 tests.
`,
  });

  const result = checkRepository({
    root,
    markdownFiles: markdownFiles(root, ['README.md']),
    packageChecks: false,
    orphanChecks: false,
  });

  assert.deepEqual(
    result.errors.map((error) => error.message),
    [
      'internal milestone token is not public documentation: D4',
      'internal milestone token is not public documentation: DL-001',
      'internal milestone token is not public documentation: F4',
      'internal milestone token is not public documentation: INV-3',
      'internal milestone token is not public documentation: P7',
      'volatile hard-coded count is not allowed: 978 tests',
    ],
  );
});

test('public policy ignores code and comments after Unicode text', () => {
  const root = fixture({
    'README.md': `# Public guide 👋

\`D4 and 978 tests\`

\`\`\`text
DL-001 and 42 tests
\`\`\`

<!-- INV-3 and 7 tests -->

Public F4 remains visible.
`,
  });

  const result = checkRepository({
    root,
    markdownFiles: markdownFiles(root, ['README.md']),
    packageChecks: false,
    orphanChecks: false,
  });

  assert.deepEqual(
    result.errors.map((error) => error.message),
    ['internal milestone token is not public documentation: F4'],
  );
});

test('published package documentation cannot use repository-relative escapes', () => {
  const root = fixture({
    'docs/guide.md': '# Guide\n',
    'packages/breakwater/package.json': JSON.stringify({
      name: '@proofoftech/breakwater',
      files: ['dist', 'README.md'],
      exports: { '.': './dist/index.js' },
    }),
    'packages/breakwater/README.md': `# Breakwater

\`@proofoftech/breakwater\`

[Repository guide](../../docs/guide.md)
`,
    'packages/breakwater/src/index.ts': 'export {};\n',
    'packages/breakwater/typedoc.json': JSON.stringify({
      entryPoints: ['src/index.ts'],
    }),
  });

  const result = checkRepository({
    root,
    markdownFiles: markdownFiles(root, [
      'docs/guide.md',
      'packages/breakwater/README.md',
    ]),
    orphanChecks: false,
  });

  assert.equal(result.errors.length, 1);
  assert.match(result.errors[0].message, /leaves the published package/);
});

test('every manifest export must appear in the README and TypeDoc', () => {
  const root = fixture({
    'packages/breakwater/package.json': JSON.stringify({
      name: '@proofoftech/breakwater',
      files: ['dist', 'README.md'],
      exports: {
        '.': './dist/index.js',
        './feature': './dist/feature/index.js',
      },
    }),
    'packages/breakwater/README.md': '# @proofoftech/breakwater\n',
    'packages/breakwater/src/index.ts': 'export {};\n',
    'packages/breakwater/src/feature/index.ts': 'export {};\n',
    'packages/breakwater/typedoc.json': JSON.stringify({
      entryPoints: ['src/index.ts'],
    }),
  });

  const result = checkRepository({
    root,
    markdownFiles: markdownFiles(root, ['packages/breakwater/README.md']),
    orphanChecks: false,
  });

  assert.deepEqual(
    result.errors.map((error) => error.message),
    [
      'README does not name public export @proofoftech/breakwater/feature',
      'TypeDoc does not cover export ./feature (src/feature/index.ts)',
    ],
  );
});

test('package READMEs state manifest-backed engine and peer ranges', () => {
  const root = fixture({
    'packages/breakwater/package.json': JSON.stringify({
      name: '@proofoftech/breakwater',
      engines: { node: '>=22' },
      peerDependencies: {
        '@mastra/core': '^1.50.0',
        react: '>=18 <20',
      },
    }),
    'packages/breakwater/README.md': `# Breakwater

Node >=20

@mastra/core ^1.49.0

react >=18 <20
`,
  });

  const result = checkRepository({
    root,
    markdownFiles: markdownFiles(root, ['packages/breakwater/README.md']),
    orphanChecks: false,
  });

  assert.deepEqual(
    result.errors.map((error) => error.message),
    [
      'README does not state manifest Node engine >=22',
      'README does not state manifest peer range @mastra/core ^1.50.0',
    ],
  );
});

test('external URL collection does not join nested badge destinations', () => {
  const root = fixture({
    'README.md': `[![CI](https://img.example/ci.svg)](https://github.example/actions)

Bare: https://docs.example/guide.
`,
  });

  assert.deepEqual(collectExternalUrls(markdownFiles(root, ['README.md'])), [
    'https://docs.example/guide',
    'https://github.example/actions',
    'https://img.example/ci.svg',
  ]);
});

test('external checker distinguishes success, hard failure, and indeterminate status', async () => {
  const statuses = new Map([
    ['https://docs.example/ok', 204],
    ['https://docs.example/private', 403],
    ['https://docs.example/missing', 404],
  ]);
  const fetchImpl = async (url) => ({ status: statuses.get(url) ?? 500 });

  assert.deepEqual(
    await checkExternalUrl('https://docs.example/ok', {
      fetchImpl,
      retries: 0,
    }),
    {
      kind: 'ok',
      status: 204,
      url: 'https://docs.example/ok',
    },
  );
  assert.deepEqual(
    await checkExternalUrl('https://docs.example/private', {
      fetchImpl,
      retries: 0,
    }),
    {
      kind: 'warning',
      status: 403,
      url: 'https://docs.example/private',
    },
  );
  assert.deepEqual(
    await checkExternalUrl('https://docs.example/missing', {
      fetchImpl,
      retries: 0,
    }),
    {
      kind: 'error',
      status: 404,
      url: 'https://docs.example/missing',
    },
  );
});
