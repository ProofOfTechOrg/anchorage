import { readFile } from 'node:fs/promises';

const html = await readFile(
  new URL('../dist/index.html', import.meta.url),
  'utf8',
);

const requiredFragments = [
  '<title>Anchorage — Guardrails and durable approvals for Mastra</title>',
  'name="description"',
  'name="robots" content="index, follow"',
  'rel="canonical" href="https://anchorage.proofoftech.org/"',
  'property="og:image"',
  'name="twitter:card" content="summary_large_image"',
];

for (const fragment of requiredFragments) {
  if (!html.includes(fragment)) {
    throw new Error(`Public metadata is missing ${JSON.stringify(fragment)}`);
  }
}

const robots = await readFile(
  new URL('../dist/robots.txt', import.meta.url),
  'utf8',
);
if (
  !robots.includes('Allow: /') ||
  !robots.includes('https://anchorage.proofoftech.org/sitemap.xml')
) {
  throw new Error('robots.txt does not expose the public sitemap');
}

const sitemap = await readFile(
  new URL('../dist/sitemap.xml', import.meta.url),
  'utf8',
);
if (!sitemap.includes('<loc>https://anchorage.proofoftech.org/</loc>')) {
  throw new Error('sitemap.xml does not contain the canonical public URL');
}

const card = await readFile(new URL('../dist/og-card.png', import.meta.url));
if (
  card.subarray(1, 4).toString('ascii') !== 'PNG' ||
  card.readUInt32BE(16) !== 1200 ||
  card.readUInt32BE(20) !== 630
) {
  throw new Error('og-card.png must be a 1200x630 PNG');
}
