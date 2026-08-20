// Pre-flight for the ordered publish: spawn the real command for every
// prerequisite with `--dry-run` and require exit 0.
//
// Why a spawn and not another assertion: publish-ordered.test.mjs can pin the
// argv's SHAPE, but only pnpm and npm can say whether they ACCEPT it. They did
// not — see the `publishInvocation` doc block in publish-ordered.mjs for the
// argv-forwarding defect this exists to catch, which reached main unexercised
// and published nothing.
//
// COVERAGE, precisely: `--dry-run` runs prepack, packs the tarball, and
// parses the full argv through `npm publish`, then stops. It makes no registry
// request at all — verified by pointing npm at an unroutable registry, which
// still exits 0 immediately. That is what makes it safe to run on every build
// with no credentials, and it is also the limit: this cannot catch
// authentication, scope permissions, provenance/OIDC, or an already-published
// version. Those remain first-run-on-main risks, and all of them fail loudly.
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
  publishInvocation,
} from './publish-ordered.mjs';

let failed = false;

for (const target of PUBLISH_PREREQUISITES) {
  const { args, cwd } = publishInvocation(target, { dryRun: true });
  const printable = `pnpm ${args.join(' ')} (cwd: ${cwd})`;
  const result = command('pnpm', args, { capture: true, cwd });
  if (result.status === 0) {
    console.log(`ok ${target.name}: ${printable}`);
    continue;
  }
  failed = true;
  console.error(`FAIL ${target.name}: ${printable}`);
  console.error(failureReason(result));
  console.error(`${result.stdout}${result.stderr}`);
}

if (failed) {
  console.error(
    'The release publish command is malformed. It would fail on main after the version PR merges, with nothing published.',
  );
  process.exit(1);
}

console.log(
  `publish invocation verified for ${PUBLISH_PREREQUISITES.length} prerequisite package(s)`,
);
