// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile, realpath, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join, relative, resolve, sep } from 'node:path';

const PACKAGE_NAME = '@proofoftech/fleet-control';
const ENTRY_NAME = `${PACKAGE_NAME}/cloudflare-control-plane`;

const VALUE_NAMES = [
  'ActiveRouteAttestationError',
  'CleanupAdvanceCapabilityError',
  'CleanupAdvanceRestartError',
  'CleanupAdvanceTokenDeploymentError',
  'CleanupAdvanceTokenError',
  'CleanupAdvanceTokenFutureError',
  'CleanupAdvanceTokenOperationError',
  'D1CloudflareApiRateCoordinator',
  'D1FleetStateDatabase',
  'DecommissionAdvanceCapabilityError',
  'DecommissionAdvanceRestartError',
  'DecommissionAdvanceTokenDeploymentError',
  'DecommissionAdvanceTokenError',
  'DecommissionAdvanceTokenFutureError',
  'DecommissionAdvanceTokenOperationError',
  'FleetAuditAdvanceCapabilityError',
  'FleetInventoryAdvanceCapabilityError',
  'FleetInventoryFindingValueError',
  'FleetInventoryRunTokenError',
  'FleetInventoryRunTokenFutureError',
  'FleetInventoryRunTokenOperationError',
  'FleetInventoryStateError',
  'FleetMigrationAdvanceCapabilityError',
  'FleetOperationStateError',
  'FleetOperationStoreCapabilityError',
  'FleetOperationTokenError',
  'FleetOperationTokenFutureError',
  'FleetOperationTokenKindError',
  'FleetOperationTokenOperationError',
  'ProvisioningError',
  'R2DatabaseExportStore',
  'WorkerDeploymentError',
  'createCloudflareControlPlane',
  'deploymentSpecDigest',
  'generateDeploymentSecrets',
];

const TYPE_NAMES = [
  'ActiveRouteAttestation',
  'ApplicationBindingTopology',
  'ApplicationR2Binding',
  'ApplicationR2Resource',
  'AttestConvergedActiveRouteOptions',
  'BackendSwitchApplicationR2Progress',
  'BackendSwitchCandidateSnapshot',
  'BackendSwitchDecommissionRelease',
  'BackendSwitchDecommissionRouteTarget',
  'BackendSwitchDecommissionSnapshot',
  'BackendSwitchIntent',
  'BackendSwitchSubphase',
  'BridgeMutationPlan',
  'BridgeSnapshot',
  'CleanupAdvanceAction',
  'CleanupAdvanceCapability',
  'CleanupAdvanceIntent',
  'CleanupAdvanceResult',
  'CleanupAdvanceState',
  'CleanupAdvanceToken',
  'CleanupAttachmentProgress',
  'CleanupAttachmentPurpose',
  'CleanupAttachmentScan',
  'CleanupAuthority',
  'CleanupReceiptEvidence',
  'CleanupTerminalReceipt',
  'CloudflareAdvanceCleanupDeploymentOptions',
  'CloudflareAdvanceDecommissionDeploymentOptions',
  'CloudflareAdvanceFleetAuditOptions',
  'CloudflareAdvanceFleetInventoryOptions',
  'CloudflareAdvanceFleetMigrationOptions',
  'CloudflareApiRateCoordinator',
  'CloudflareControlPlane',
  'CloudflareControlPlaneOptions',
  'CloudflareDeploymentSpec',
  'CloudflareFleetInventoryAdvanceAction',
  'CloudflareFleetInventoryOptions',
  'CloudflareFleetMigrationItemsPage',
  'CloudflareFleetOperationPageOptions',
  'CloudflareProvisionDeploymentOptions',
  'D1CloudflareApiRateCoordinatorOptions',
  'D1Migration',
  'DatabaseExport',
  'DatabaseExportIntegrity',
  'DatabaseExportReceiptIdentity',
  'DecommissionAdvanceAction',
  'DecommissionAdvanceCapability',
  'DecommissionAdvanceIntent',
  'DecommissionAdvanceResult',
  'DecommissionAdvanceToken',
  'DecommissionAttachmentProgress',
  'DecommissionAttachmentPurpose',
  'DecommissionAttachmentScanEvidence',
  'DecommissionBlockedAttachment',
  'DecommissionIntentCommon',
  'DecommissionOperationIdentity',
  'DecommissionOperationMode',
  'DecommissionRecordIdentity',
  'DecommissionResult',
  'DeploymentApplicationBindings',
  'DeploymentEgressPolicy',
  'DeploymentSecrets',
  'DeploymentSpec',
  'DigestStreamConstructor',
  'DriftFinding',
  'DurableDatabaseExportStore',
  'DurableObjectBindingInventory',
  'DurableObjectMigration',
  'ExternalMigrationIntent',
  'ExternalMigrationSubphase',
  'ExternalPlatformResources',
  'ExternalPlatformTargetDescription',
  'ExternalReleaseSnapshot',
  'ExternalReleaseTopology',
  'FixedLengthStreamConstructor',
  'FleetAuditAdvanceAction',
  'FleetAuditAdvanceCapability',
  'FleetAuditAdvanceResult',
  'FleetAuditFindingsPage',
  'FleetAuditResultRef',
  'FleetAuditStage',
  'FleetInventoryAdvanceCapability',
  'FleetInventoryAdvanceResult',
  'FleetInventoryDeployment',
  'FleetInventoryFinding',
  'FleetInventoryGenerationRef',
  'FleetInventoryRowKind',
  'FleetInventoryRunToken',
  'FleetMigrationAdvanceAction',
  'FleetMigrationAdvanceResult',
  'FleetMigrationItem',
  'FleetMigrationPlanEntry',
  'FleetMigrationResultRef',
  'FleetMigrationStep',
  'FleetOperationFailure',
  'FleetOperationKind',
  'FleetOperationToken',
  'FleetRecord',
  'FleetResourceInventory',
  'FleetSettlementContext',
  'FleetSettlementEntry',
  'FleetSettlementHost',
  'FleetStateDatabase',
  'HostRoutingTarget',
  'InitialExecutionFenceState',
  'InvocationAuthorityCarrier',
  'MaintenanceHealth',
  'NormalDecommissionLifecyclePhase',
  'ObservedActiveRoute',
  'PlainBackendSnapshot',
  'PlatformWorkerSnapshot',
  'ProvisioningBackendKind',
  'ProvisioningPhase',
  'ProvisioningResult',
  'R2DatabaseExportStoreOptions',
  'R2DatabaseExportStoreStreamPrimitives',
  'R2Jurisdiction',
  'WorkerModule',
  'WorkerZoneRoute',
];

const METHOD_NAMES = [
  'abandonFleetAuditOperation',
  'abandonFleetMigrationOperation',
  'advanceCleanupDeployment',
  'advanceDecommissionDeployment',
  'advanceFleetAudit',
  'advanceFleetInventory',
  'advanceFleetMigration',
  'getDeployment',
  'latestFinalizedInventoryGeneration',
  'provisionDeployment',
  'pruneCleanupReceipts',
  'pruneFleetOperations',
  'pruneInventoryGenerations',
  'readCleanupReceipt',
  'readFleetAuditFindingsPage',
  'readFleetInventoryGeneration',
  'readFleetMigrationItemsPage',
];

const FORBIDDEN_TYPES = new Set([
  'CloudflareProvisioningClient',
  'CloudflareApiPlainWorkerBackend',
  'PlainWorkerBackend',
  'ProvisioningBackend',
  'PlainWorkerProvisioningApi',
  'BackendSwitchProvider',
  'FinalizedOrdinaryStateProvider',
  'FleetStateStore',
  'PlatformPlaneStateStore',
  'FleetStateLease',
  'FleetInventoryProviderContext',
  'FleetInventoryRunStore',
  'FleetInventoryLease',
  'FleetOperationStore',
  'FleetOperationLease',
  'ProcessLocalCloudflareApiRateCoordinator',
  'WorkersForPlatformsBackend',
  'WorkersForPlatformsBackendSwitchProvider',
  'WranglerLoopBackend',
  'FileSystemDatabaseExportStore',
]);

function inspectPublicTypes(ts, program, entryPath, installedRoot) {
  const checker = program.getTypeChecker();
  const entry = program.getSourceFile(entryPath);
  assert.ok(
    entry,
    `installed declaration is absent from program: ${entryPath}`,
  );
  const exports = checker.getExportsOfModule(
    checker.getSymbolAtLocation(entry),
  );
  assert.deepEqual(
    exports.map((symbol) => symbol.name).sort(),
    [...VALUE_NAMES, ...TYPE_NAMES].sort(),
    'curated declaration export table differs',
  );
  const resolveSymbol = (symbol) =>
    symbol?.flags & ts.SymbolFlags.Alias
      ? checker.getAliasedSymbol(symbol)
      : symbol;
  const exported = new Set(exports.map(resolveSymbol));
  const visitedSymbols = new Set();
  const visitedTypes = new Set();
  const reached = [];
  const namedFlags =
    ts.SymbolFlags.TypeAlias |
    ts.SymbolFlags.Interface |
    ts.SymbolFlags.Class |
    ts.SymbolFlags.Enum;
  const isLocal = (node) =>
    node?.getSourceFile().fileName.startsWith(`${installedRoot}${sep}`);
  const isPublic = (node) =>
    !(node.name && ts.isPrivateIdentifier(node.name)) &&
    !(
      ts.getCombinedModifierFlags(node) &
      (ts.ModifierFlags.Private | ts.ModifierFlags.Protected)
    );

  function followSymbol(candidate, trail) {
    const symbol = resolveSymbol(candidate);
    if (!symbol || visitedSymbols.has(symbol)) return;
    const declarations = (symbol.declarations ?? []).filter(isLocal);
    if (!declarations.length) return;
    visitedSymbols.add(symbol);
    if (symbol.flags & namedFlags) {
      const declaration = declarations[0];
      const file = declaration.getSourceFile();
      const location = `${relative(installedRoot, file.fileName)}:${file.getLineAndCharacterOfPosition(declaration.getStart()).line + 1}`;
      assert.ok(
        !FORBIDDEN_TYPES.has(symbol.name),
        `forbidden public capability ${symbol.name} at ${location}: ${trail.join(' -> ')}`,
      );
      assert.ok(
        exported.has(symbol),
        `missing public type export ${symbol.name} at ${location}: ${trail.join(' -> ')}`,
      );
      reached.push({ name: symbol.name, location, trail });
    }
    for (const declaration of declarations) {
      followDeclaration(declaration, [...trail, symbol.name]);
    }
  }

  function followTypeNode(node, trail) {
    if (!node) return;
    const reference = ts.isTypeReferenceNode(node)
      ? node.typeName
      : ts.isTypeQueryNode(node)
        ? node.exprName
        : ts.isExpressionWithTypeArguments(node)
          ? node.expression
          : ts.isImportTypeNode(node)
            ? node.qualifier
            : undefined;
    if (reference) {
      followSymbol(checker.getSymbolAtLocation(reference), [
        ...trail,
        reference.getText(),
      ]);
    }
    ts.forEachChild(node, (child) => followTypeNode(child, trail));
  }

  function followSignature(signature, trail) {
    if (!signature) return;
    for (const parameter of [
      ...signature.parameters,
      ...(signature.thisParameter ? [signature.thisParameter] : []),
    ]) {
      const declaration =
        parameter.valueDeclaration ?? parameter.declarations?.[0];
      if (declaration) {
        followType(checker.getTypeOfSymbolAtLocation(parameter, declaration), [
          ...trail,
          `parameter ${parameter.name}`,
        ]);
      }
    }
    followType(checker.getReturnTypeOfSignature(signature), [
      ...trail,
      'return',
    ]);
    followType(checker.getTypePredicateOfSignature(signature)?.type, trail);
  }

  function followType(type, trail) {
    if (!type || visitedTypes.has(type)) return;
    visitedTypes.add(type);
    followSymbol(type.aliasSymbol, trail);
    const symbol = type.getSymbol();
    if (symbol?.flags & namedFlags) followSymbol(symbol, trail);
    for (const argument of type.aliasTypeArguments ?? [])
      followType(argument, trail);
    if (type.flags & (ts.TypeFlags.Union | ts.TypeFlags.Intersection)) {
      for (const item of type.types) followType(item, trail);
    }
    if (type.flags & ts.TypeFlags.TypeParameter) {
      followType(checker.getBaseConstraintOfType(type), trail);
    }
    if (type.flags & ts.TypeFlags.IndexedAccess) {
      followType(type.objectType, trail);
      followType(type.indexType, trail);
    }
    if (!(type.flags & ts.TypeFlags.Object)) return;
    if (type.objectFlags & ts.ObjectFlags.Reference) {
      for (const argument of checker.getTypeArguments(type))
        followType(argument, trail);
    }
    if (
      !symbol?.declarations?.some(isLocal) &&
      !type.aliasSymbol?.declarations?.some(isLocal)
    )
      return;
    for (const signature of [
      ...type.getCallSignatures(),
      ...type.getConstructSignatures(),
    ]) {
      if (isLocal(signature.declaration)) followSignature(signature, trail);
    }
    for (const property of checker.getPropertiesOfType(type)) {
      const declaration = property.declarations?.find(
        (node) => isLocal(node) && isPublic(node),
      );
      if (declaration) {
        followType(checker.getTypeOfSymbolAtLocation(property, declaration), [
          ...trail,
          `property ${property.name}`,
        ]);
      }
    }
    for (const index of checker.getIndexInfosOfType(type))
      followType(index.type, trail);
    if (type.objectFlags & ts.ObjectFlags.ClassOrInterface) {
      for (const base of checker.getBaseTypes(type) ?? [])
        followType(base, trail);
    }
  }

  function followDeclaration(declaration, trail) {
    if (!ts.isParameter(declaration) && !isPublic(declaration)) return;
    for (const parameter of declaration.typeParameters ?? []) {
      followTypeNode(parameter.constraint, trail);
      followTypeNode(parameter.default, trail);
    }
    for (const heritage of declaration.heritageClauses ?? []) {
      for (const type of heritage.types) followTypeNode(type, trail);
    }
    if (
      ts.isClassDeclaration(declaration) ||
      ts.isInterfaceDeclaration(declaration)
    ) {
      for (const member of declaration.members) {
        followDeclaration(member, [
          ...trail,
          member.name?.getText() ?? ts.SyntaxKind[member.kind],
        ]);
      }
      return;
    }
    if (ts.isTypeAliasDeclaration(declaration)) {
      followTypeNode(declaration.type, trail);
      followType(checker.getTypeFromTypeNode(declaration.type), trail);
      return;
    }
    followTypeNode(declaration.type, trail);
    if (
      ts.isFunctionLike(declaration) ||
      ts.isCallSignatureDeclaration(declaration) ||
      ts.isConstructSignatureDeclaration(declaration)
    ) {
      for (const parameter of declaration.parameters)
        followDeclaration(parameter, trail);
      followSignature(checker.getSignatureFromDeclaration(declaration), trail);
      return;
    }
    if (declaration.name) {
      const symbol = checker.getSymbolAtLocation(declaration.name);
      if (symbol)
        followType(
          checker.getTypeOfSymbolAtLocation(symbol, declaration),
          trail,
        );
    }
  }

  for (const symbol of exports) followSymbol(symbol, [`export ${symbol.name}`]);
  for (const name of TYPE_NAMES) {
    assert.ok(
      resolveSymbol(exports.find((symbol) => symbol.name === name)).flags &
        ts.SymbolFlags.Type,
      `${name} must be a type export`,
    );
  }
  return reached.sort((left, right) => left.name.localeCompare(right.name));
}

function workerProgram() {
  return `import { ${VALUE_NAMES.join(', ')} } from '${ENTRY_NAME}';
import type { ${TYPE_NAMES.join(', ')} } from '${ENTRY_NAME}';
import type { D1Database, R2Bucket } from '@cloudflare/workers-types';
export type PackedControlPlaneTypes = [${TYPE_NAMES.join(', ')}];
void [${VALUE_NAMES.join(', ')}];
declare const fleetDatabase: D1Database;
declare const quotaDatabase: D1Database;
declare const bucket: R2Bucket;
const options: CloudflareControlPlaneOptions = {
  accountId: 'packed-account', apiToken: 'inert-token',
  fleetDatabase, quotaDatabase, quotaScope: 'packed-quota',
  databaseExports: {
    bucket, bucketName: 'packed-exports',
    streams: { DigestStream: crypto.DigestStream, FixedLengthStream },
    randomUUID: () => crypto.randomUUID(),
  },
};
const plane: CloudflareControlPlane = createCloudflareControlPlane(options);
type ExpectedMethods = ${METHOD_NAMES.map((name) => `'${name}'`).join(' | ')};
const noMissingMethods: Exclude<ExpectedMethods, keyof CloudflareControlPlane> extends never ? true : false = true;
const noExtraMethods: Exclude<keyof CloudflareControlPlane, ExpectedMethods> extends never ? true : false = true;
void [plane, noMissingMethods, noExtraMethods];
`;
}

function runtimeProgram(runtimeEntry) {
  return `import assert from 'node:assert/strict';
import { realpath } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import * as surface from '${ENTRY_NAME}';
assert.equal(await realpath(fileURLToPath(import.meta.resolve('${ENTRY_NAME}'))), ${JSON.stringify(runtimeEntry)});
assert.deepEqual(Object.keys(surface).sort(), ${JSON.stringify(VALUE_NAMES)});
for (const name of ${JSON.stringify(VALUE_NAMES)}) assert.equal(typeof surface[name], 'function', name);
for (const name of ${JSON.stringify(VALUE_NAMES.filter((name) => name.endsWith('Error')))}) {
  assert.ok(surface[name].prototype instanceof Error, name);
  assert.deepEqual(Reflect.ownKeys(surface[name].prototype), ['constructor'], name);
}
for (const [name, methods] of Object.entries({
  D1FleetStateDatabase: ['batch', 'constructor', 'execute', 'query'],
  D1CloudflareApiRateCoordinator: ['acquire', 'constructor'],
  R2DatabaseExportStore: ['constructor', 'write'],
})) assert.deepEqual(Reflect.ownKeys(surface[name].prototype).sort(), methods, name);
const unexpected = () => { throw new Error('packed surface shape probe performed I/O'); };
const database = { prepare: unexpected, batch: unexpected };
const plane = surface.createCloudflareControlPlane({
  accountId: 'packed-account', apiToken: 'inert-token',
  fleetDatabase: database, quotaDatabase: database, quotaScope: 'packed-quota',
  fetch: unexpected, maintenanceFetch: unexpected,
  databaseExports: {
    bucket: { put: unexpected, get: unexpected, delete: unexpected },
    bucketName: 'packed-exports',
    streams: { DigestStream: class {}, FixedLengthStream: class {} },
    randomUUID: () => crypto.randomUUID(),
  },
});
assert.equal(Object.getPrototypeOf(plane), Object.prototype);
assert.equal(Object.isFrozen(plane), true);
assert.deepEqual(Reflect.ownKeys(plane).sort(), ${JSON.stringify(METHOD_NAMES)});
for (const method of ${JSON.stringify(METHOD_NAMES)}) {
  const descriptor = Object.getOwnPropertyDescriptor(plane, method);
  assert.equal(typeof descriptor.value, 'function', method);
  assert.equal(descriptor.writable, false, method);
  assert.equal(descriptor.configurable, false, method);
}
`;
}

export async function verifyControlPlanePackedSurface({
  consumerDirectory,
  packageRoot,
}) {
  const consumer = await realpath(consumerDirectory);
  const consumerRequire = createRequire(join(consumer, 'package.json'));
  const manifestPath = await realpath(
    consumerRequire.resolve(`${PACKAGE_NAME}/package.json`),
  );
  const installedRoot = dirname(manifestPath);
  assert.ok(
    installedRoot.startsWith(`${consumer}${sep}`),
    'Fleet must resolve inside the installed consumer',
  );
  assert.notEqual(
    installedRoot,
    await realpath(packageRoot),
    'Fleet must not resolve to its workspace implementation',
  );
  const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
  assert.deepEqual(manifest.exports['./cloudflare-control-plane'], {
    types: './dist/cloudflare-control-plane.d.ts',
    default: './dist/cloudflare-control-plane.js',
  });
  const runtimeEntry = await realpath(consumerRequire.resolve(ENTRY_NAME));
  assert.equal(
    runtimeEntry,
    join(installedRoot, 'dist/cloudflare-control-plane.js'),
  );
  const declarationEntry = await realpath(
    join(installedRoot, 'dist/cloudflare-control-plane.d.ts'),
  );
  const workersTypesManifest = await realpath(
    consumerRequire.resolve('@cloudflare/workers-types/package.json'),
  );
  assert.equal(
    await realpath(
      createRequire(runtimeEntry).resolve(
        '@cloudflare/workers-types/package.json',
      ),
    ),
    workersTypesManifest,
    'Fleet and the Worker consumer must resolve the same Workers type peer',
  );
  const toolingRequire = createRequire(
    join(resolve(packageRoot), 'package.json'),
  );
  const ts = toolingRequire('typescript');
  const artifacts = {
    worker: join(consumer, 'control-plane-worker.ts'),
    config: join(consumer, 'tsconfig.control-plane-worker.json'),
    runtime: join(consumer, 'control-plane-runtime.mjs'),
    report: join(consumer, 'control-plane-surface-report.json'),
  };
  const configuration = {
    compilerOptions: {
      target: 'ES2022',
      lib: ['ES2022'],
      module: 'NodeNext',
      moduleResolution: 'NodeNext',
      strict: true,
      noEmit: true,
      skipLibCheck: false,
      types: ['@cloudflare/workers-types'],
    },
    files: ['control-plane-worker.ts'],
  };
  await writeFile(artifacts.worker, workerProgram());
  await writeFile(
    artifacts.config,
    `${JSON.stringify(configuration, null, 2)}\n`,
  );
  await writeFile(artifacts.runtime, runtimeProgram(runtimeEntry));
  const parsed = ts.parseJsonConfigFileContent(configuration, ts.sys, consumer);
  const program = ts.createProgram(parsed.fileNames, parsed.options);
  const resolution = ts.resolveModuleName(
    ENTRY_NAME,
    artifacts.worker,
    parsed.options,
    ts.sys,
  ).resolvedModule;
  assert.ok(resolution, 'Worker program cannot resolve the installed subpath');
  assert.equal(await realpath(resolution.resolvedFileName), declarationEntry);
  const diagnostics = [...parsed.errors, ...ts.getPreEmitDiagnostics(program)];
  const formattedDiagnostics = ts.formatDiagnosticsWithColorAndContext(
    diagnostics,
    {
      getCanonicalFileName: (name) => name,
      getCurrentDirectory: () => consumer,
      getNewLine: () => '\n',
    },
  );
  const identity = {
    packageRoot: installedRoot,
    manifest: manifestPath,
    runtimeEntry,
    declarationEntry,
    workersTypesManifest,
    workersTypesVersion: JSON.parse(
      await readFile(workersTypesManifest, 'utf8'),
    ).version,
    runtimeSha256: createHash('sha256')
      .update(await readFile(runtimeEntry))
      .digest('hex'),
    declarationSha256: createHash('sha256')
      .update(await readFile(declarationEntry))
      .digest('hex'),
    typescriptVersion: ts.version,
  };
  await writeFile(
    artifacts.report,
    `${JSON.stringify({ status: 'checking', identity, artifacts, diagnostics: formattedDiagnostics }, null, 2)}\n`,
  );
  assert.equal(diagnostics.length, 0, formattedDiagnostics);
  const reached = inspectPublicTypes(
    ts,
    program,
    resolution.resolvedFileName,
    installedRoot,
  );
  execFileSync(process.execPath, [artifacts.runtime], {
    cwd: consumer,
    stdio: 'pipe',
  });
  await writeFile(
    artifacts.report,
    `${JSON.stringify({ status: 'passed', identity, artifacts, valueNames: VALUE_NAMES, typeNames: TYPE_NAMES, reached, diagnostics: [] }, null, 2)}\n`,
  );
  return { identity, artifacts };
}
