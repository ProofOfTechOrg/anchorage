// SPDX-License-Identifier: Apache-2.0

export async function closeFixtures(
  closers: readonly (() => void | Promise<void>)[],
  postChecks: readonly (() => void | Promise<void>)[],
  label: string,
): Promise<void> {
  const closerResults = await Promise.allSettled(
    closers.map(async (close) => close()),
  );
  const postCheckResults = await Promise.allSettled(
    postChecks.map(async (postCheck) => postCheck()),
  );
  const failures = [...closerResults, ...postCheckResults].flatMap((result) =>
    result.status === 'rejected' ? [result.reason] : [],
  );
  if (failures.length > 0) throw new AggregateError(failures, label);
}
