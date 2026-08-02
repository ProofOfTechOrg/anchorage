// SPDX-License-Identifier: Apache-2.0

/**
 * Preserve ordinary Mastra config keys while remapping names inherited from
 * Object.prototype. Mastra copies config into plain-object registries, where
 * keys such as `__proto__` and `constructor` otherwise appear occupied.
 */
export function mastraRegistryEntries<T>(
  entries: Iterable<readonly [string, T]>,
  namespace: string,
): Array<[string, T]> {
  const values = [...entries];
  const logicalIds = new Set(values.map(([id]) => id));
  return values.map(([id, value]) => {
    if (!Object.hasOwn(Object.prototype, id)) return [id, value];
    let registrationKey = `${namespace}:${id}`;
    while (
      logicalIds.has(registrationKey) ||
      Object.hasOwn(Object.prototype, registrationKey)
    ) {
      registrationKey = `${namespace}:${registrationKey}`;
    }
    return [registrationKey, value];
  });
}
