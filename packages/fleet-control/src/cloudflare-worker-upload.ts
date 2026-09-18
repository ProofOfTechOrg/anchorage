// SPDX-License-Identifier: Apache-2.0

export function namedWorkerUploadBody(
  files: readonly File[],
  metadata: string,
): Record<string, string | File> {
  const parts = new Map<string, string | File>([['metadata', metadata]]);
  for (const file of files) {
    if (parts.has(file.name)) {
      throw new Error(
        `Worker upload part '${file.name}' is duplicated or reserved`,
      );
    }
    parts.set(file.name, file);
  }
  return Object.fromEntries(parts);
}
