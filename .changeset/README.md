# Changesets

Version and changelog management for the publishable packages
(`@proofoftech/breakwater`, `@proofoftech/flowsafe`; `showcase` is private and
never publishes).

Every PR that changes published behavior adds a changeset:

```bash
pnpm exec changeset
```

Pick the affected package(s) and a semver bump, describe the change (this text
becomes the CHANGELOG entry). On merge to `dev`, the version workflow
(`.github/workflows/version.yml`) opens/updates a standing "Version Packages"
PR against dev that applies the pending bumps. Releasing = merge that PR into
dev, then immediately cut and merge the release PR (`dev` → `main`);
`.github/workflows/release.yml` then publishes the bumped versions to npm with
provenance (and refuses, loudly, if an unversioned changeset reached main).

Docs: https://github.com/changesets/changesets
