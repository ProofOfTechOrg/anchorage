// SPDX-License-Identifier: Apache-2.0

const projectSelector =
  /(?:^|\s)--project(?:=|\s+)(?:"([^"]+)"|'([^']+)'|([^\s]+))/gu;

/** Each `--project` selector of a Vitest command, unquoted, with any `!` and `*` kept. */
export function projectSelectors(command) {
  return [...command.matchAll(projectSelector)].map((match) =>
    match.slice(1).find((candidate) => candidate !== undefined),
  );
}

/** The selectors that name one project: neither negated nor a wildcard. */
export function explicitProjects(command) {
  return projectSelectors(command).filter(
    (selector) => !selector.startsWith('!') && !selector.includes('*'),
  );
}
