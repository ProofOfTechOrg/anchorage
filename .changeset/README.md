# Changesets

Version and changelog management for the publishable packages
(`@proofoftech/breakwater`, `@proofoftech/flowsafe`; `showcase` is private and
never publishes).

Every PR that changes published behavior adds a changeset:

```bash
pnpm exec changeset
```

Pick the affected package(s) and a semver bump, describe the change (this text
becomes the CHANGELOG entry). On merge to `main`, the release workflow
(`.github/workflows/release.yml`) opens/updates a "Version Packages" PR that
applies the pending bumps; merging THAT PR publishes to npm with provenance.

Docs: https://github.com/changesets/changesets
