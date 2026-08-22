// Pre-flight for the ordered publish: spawn the real command for every
// prerequisite with `--dry-run` and require exit 0.
//
// Why a spawn and not another assertion: publish-ordered.test.mjs can pin the
// argv's SHAPE, but only pnpm and npm can say whether they ACCEPT it. They did
// not — see the `publishInvocation` doc block in publish-ordered.mjs for the
// argv-forwarding defect this exists to catch, which reached main unexercised
// and published nothing.
//
// COVERAGE, precisely: the grammar check validates every prerequisite peer
// edge, and `--dry-run` runs prepack, packs the tarball, and parses the full argv
// through `npm publish`, then stops. It makes no registry request at all —
// verified by pointing npm at an unroutable registry, which still exits 0
// immediately. That is what makes it safe to run on every build with no
// credentials, and it is also the limit: this cannot catch authentication,
// scope permissions, provenance/OIDC, or an already-published version. Those
// remain first-run-on-main risks, and all of them fail loudly.
//
// The PR-time check deliberately skips peer-version containment. Between a
// floor raise and the Version Packages PR, the source tree legitimately has
// the old package version; the release-time publish gate checks it after bumps.
//
// It also covers only PUBLISH_PREREQUISITES, not the `changeset publish`
// remainder, which has no dry-run of its own. Accepted deliberately: that
// command was the release workflow's direct `publish:` input before 09a4406 and
// published from main repeatedly, so only the wrapper around it is new, and the
// wrapper passes it no arguments. Revisit if the remainder ever grows flags.

import {
  command,
  failureReason,
  PUBLISH_PREREQUISITES,
  peerFloorGrammarViolations,
  prerequisiteManifests,
  prerequisitePeerEdges,
  publishInvocation,
} from './publish-ordered.mjs';

const manifests = prerequisiteManifests();
// Version containment belongs to publishRelease after pending changesets bump versions.
const peerFloorGrammar = peerFloorGrammarViolations(manifests);
const peerEdges = [...prerequisitePeerEdges(manifests)];
let invocationFailed = false;

for (const { ownerName, message } of peerFloorGrammar) {
  const { directory } = PUBLISH_PREREQUISITES.find(
    ({ name }) => name === ownerName,
  );
  console.error(`FAIL ${directory}/package.json: ${message}`);
}

for (const target of PUBLISH_PREREQUISITES) {
  const { args, cwd } = publishInvocation(target, { dryRun: true });
  const printable = `pnpm ${args.join(' ')} (cwd: ${cwd})`;
  const result = command('pnpm', args, { capture: true, cwd });
  if (result.status === 0) {
    console.log(`ok ${target.name}: ${printable}`);
    continue;
  }
  invocationFailed = true;
  console.error(`FAIL ${target.name}: ${printable}`);
  console.error(failureReason(result));
  console.error(`${result.stdout}${result.stderr}`);
}

if (invocationFailed) {
  console.error(
    'The release publish command is malformed. It would fail on main after the version PR merges, with nothing published.',
  );
}

if (peerFloorGrammar.length > 0) {
  console.error(
    'The prerequisite peer-floor grammar is invalid. Release publishing would stop before any package is published.',
  );
}

if (peerFloorGrammar.length > 0 || invocationFailed) {
  process.exit(1);
}

console.log(`peer-floor grammar verified for ${peerEdges.length} edge(s)`);
console.log(
  `publish invocation verified for ${PUBLISH_PREREQUISITES.length} prerequisite package(s)`,
);
