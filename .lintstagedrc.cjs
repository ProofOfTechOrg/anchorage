// Staged-files-only autofix (pre-commit). Deliberately NOT a repo-wide
// `biome check --write`: parallel Claude/editor sessions share this worktree,
// and a sweeping autofix would touch files another session is mid-edit on.
module.exports = {
  // lint-staged passes paths, but this command checks all of .github for omissions.
  '.github/**/*.{yml,yaml}': 'pnpm github:check',
  '**/*.{ts,tsx,mjs,cjs,js,jsx,json,jsonc,css}':
    'pnpm exec biome check --write',
};
