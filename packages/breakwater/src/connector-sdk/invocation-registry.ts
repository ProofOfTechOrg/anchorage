// SPDX-License-Identifier: Apache-2.0

import type { Tool } from '@mastra/core/tools';

export interface ConnectorInvocationBoundary<
  TInput = unknown,
  TOutput = unknown,
> {
  readonly id: string;
  readonly execute: NonNullable<Tool<TInput, TOutput>['execute']>;
  readonly inputSchema: ConnectorSchemaBoundary;
  readonly outputSchema: ConnectorSchemaBoundary;
}

interface StoredConnectorInvocationBoundary {
  readonly id: string;
  readonly execute: unknown;
  readonly inputSchema: ConnectorSchemaBoundary;
  readonly outputSchema: ConnectorSchemaBoundary;
}

interface ConnectorSchemaBoundary {
  readonly schema: unknown;
  readonly standard: unknown;
  readonly validate: unknown;
  readonly jsonSchema: unknown;
  readonly jsonSchemaInput: unknown;
  readonly jsonSchemaOutput: unknown;
}

const connectorInvocations = new WeakMap<
  object,
  StoredConnectorInvocationBoundary
>();

function isObjectLike(value: unknown): value is object {
  return (
    (typeof value === 'object' && value !== null) || typeof value === 'function'
  );
}

function schemaBoundary(schema: unknown): ConnectorSchemaBoundary {
  const standard =
    isObjectLike(schema) && '~standard' in schema
      ? (schema as { '~standard': unknown })['~standard']
      : undefined;
  const standardRecord = isObjectLike(standard)
    ? (standard as Record<string, unknown>)
    : undefined;
  const jsonSchema = standardRecord?.jsonSchema;
  const jsonSchemaRecord = isObjectLike(jsonSchema)
    ? (jsonSchema as Record<string, unknown>)
    : undefined;
  return Object.freeze({
    schema,
    standard,
    validate: standardRecord?.validate,
    jsonSchema,
    jsonSchemaInput: jsonSchemaRecord?.input,
    jsonSchemaOutput: jsonSchemaRecord?.output,
  });
}

function sameSchemaBoundary(
  expected: ConnectorSchemaBoundary,
  actual: unknown,
): boolean {
  const current = schemaBoundary(actual);
  return (
    current.schema === expected.schema &&
    current.standard === expected.standard &&
    current.validate === expected.validate &&
    current.jsonSchema === expected.jsonSchema &&
    current.jsonSchemaInput === expected.jsonSchemaInput &&
    current.jsonSchemaOutput === expected.jsonSchemaOutput
  );
}

function matchesConnectorInvocationMetadata<TInput, TOutput>(
  tool: Tool<TInput, TOutput>,
  expected: StoredConnectorInvocationBoundary,
): boolean {
  try {
    return (
      expected.id === tool.id &&
      sameSchemaBoundary(expected.inputSchema, tool.inputSchema) &&
      sameSchemaBoundary(expected.outputSchema, tool.outputSchema)
    );
  } catch {
    return false;
  }
}

function boundaryOf<TInput, TOutput>(
  tool: Tool<TInput, TOutput>,
): ConnectorInvocationBoundary<TInput, TOutput> {
  if (typeof tool.execute !== 'function') {
    throw new TypeError(
      'createConnector could not establish a required execution boundary',
    );
  }
  return Object.freeze({
    id: tool.id,
    execute: tool.execute,
    inputSchema: schemaBoundary(tool.inputSchema),
    outputSchema: schemaBoundary(tool.outputSchema),
  });
}

export function registerConnectorInvocation<TInput, TOutput>(
  tool: Tool<TInput, TOutput>,
): void {
  connectorInvocations.set(tool, boundaryOf(tool));
}

export function replaceConnectorInvocation<TInput, TOutput>(
  tool: Tool<TInput, TOutput>,
  expectedExecute: NonNullable<Tool<TInput, TOutput>['execute']>,
): void {
  const current = connectorInvocations.get(tool);
  if (
    !current ||
    !matchesConnectorInvocationMetadata(tool, current) ||
    current.execute !== expectedExecute
  ) {
    throw new TypeError(
      'connector execution boundary changed before its internal decorator was installed',
    );
  }
  connectorInvocations.set(tool, boundaryOf(tool));
}

export function connectorInvocationBoundary<TInput, TOutput>(
  tool: Tool<TInput, TOutput>,
): ConnectorInvocationBoundary<TInput, TOutput> | undefined {
  return connectorInvocations.get(tool) as
    | ConnectorInvocationBoundary<TInput, TOutput>
    | undefined;
}

export function isConnectorInvocationBoundaryCurrent<TInput, TOutput>(
  tool: Tool<TInput, TOutput>,
  boundary: ConnectorInvocationBoundary<TInput, TOutput>,
): boolean {
  try {
    return (
      matchesConnectorInvocationMetadata(tool, boundary) &&
      boundary.execute === tool.execute
    );
  } catch {
    return false;
  }
}
