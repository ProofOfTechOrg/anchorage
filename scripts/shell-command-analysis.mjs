import { createRequire } from 'node:module';

import { Language, Parser } from 'web-tree-sitter';

// This checker covers pnpm executed directly by a Bash-parsed workflow run
// block and pnpm/action-setup steps handled by the YAML adapter. Package
// scripts, script files, actions, action inputs, BASH_ENV, and programs that
// may spawn pnpm are opaque. The finite model is conservative for syntax it
// cannot classify, but it is not a defence against deliberately obfuscated
// workflow text.

const require = createRequire(import.meta.url);
const GITHUB_EXPRESSION_MARKER = '\u{E000}';

await Parser.init();
const bashLanguage = await Language.load(
  require.resolve('tree-sitter-bash/tree-sitter-bash.wasm'),
);

function pnpmOptions(behavior, arity, names) {
  return names.map((name) => [name, { arity, behavior }]);
}

const INSTALL_OPTIONS = new Map([
  ...pnpmOptions('establish', 0, [
    '--aggregate-output',
    '--color',
    '--fix-lockfile',
    '--force',
    '--frozen-lockfile',
    '--ignore-scripts',
    '--no-color',
    '--no-frozen-lockfile',
    '--no-hoist',
    '--no-lockfile',
    '--no-verify-store-integrity',
    '--offline',
    '--optimistic-repeat-install',
    '--prefer-frozen-lockfile',
    '--prefer-offline',
    '--recursive',
    '--shamefully-hoist',
    '--side-effects-cache',
    '--side-effects-cache-readonly',
    '--silent',
    '--stream',
    '--strict-peer-dependencies',
    '--update-checksums',
    '--use-running-store-server',
    '--use-stderr',
    '--use-store-server',
    '--verify-store-integrity',
    '--workspace-root',
    '-r',
    '-s',
    '-w',
  ]),
  ...pnpmOptions('establish', 1, [
    '--child-concurrency',
    '--hoist-pattern',
    '--loglevel',
    '--network-concurrency',
    '--package-import-method',
    '--public-hoist-pattern',
    '--reporter',
    '--store-dir',
    '--trust-policy',
    '--trust-policy-exclude',
    '--trust-policy-ignore-after',
    '--virtual-store-dir',
  ]),
  ...pnpmOptions('neutral', 0, [
    '--dev',
    '--global',
    '--help',
    '--ignore-workspace',
    '--lockfile-only',
    '--no-optional',
    '--prod',
    '--production',
    '--resolution-only',
    '--version',
    '-D',
    '-P',
    '-g',
    '-h',
    '-v',
  ]),
  ...pnpmOptions('neutral', 1, [
    '--filter',
    '--filter-prod',
    '--modules-dir',
    '-F',
  ]),
  ...pnpmOptions('limit', 0, [
    '--bail',
    '--fail-if-no-match',
    '--ignore-pnpmfile',
    '--include-workspace-root',
    '--link-workspace-packages',
    '--merge-git-branch-lockfiles',
    '--no-bail',
    '--no-link-workspace-packages',
    '--no-shared-workspace-lockfile',
    '--no-sort',
    '--parallel',
    '--report-summary',
    '--reverse',
    '--shared-workspace-lockfile',
    '--sort',
  ]),
  ...pnpmOptions('limit', 1, [
    '--changed-files-ignore-pattern',
    '--global-dir',
    '--lockfile-dir',
    '--test-pattern',
    '--workspace-concurrency',
  ]),
]);

const ADD_OPTIONS = new Map([
  ...pnpmOptions('establish', 0, [
    '--aggregate-output',
    '--allow-build',
    '--color',
    '--ignore-scripts',
    '--no-color',
    '--no-save-exact',
    '--no-save-workspace-protocol',
    '--offline',
    '--prefer-offline',
    '--recursive',
    '--save-catalog',
    '--save-dev',
    '--save-exact',
    '--save-optional',
    '--save-peer',
    '--save-prod',
    '--save-workspace-protocol',
    '--silent',
    '--stream',
    '--use-stderr',
    '--workspace',
    '--workspace-root',
    '-D',
    '-E',
    '-O',
    '-P',
    '-r',
    '-s',
    '-w',
  ]),
  ...pnpmOptions('establish', 1, [
    '--loglevel',
    '--reporter',
    '--save-catalog-name',
    '--store-dir',
    '--virtual-store-dir',
  ]),
  ...pnpmOptions('neutral', 0, [
    '--config',
    '--dev',
    '--global',
    '--help',
    '--ignore-workspace',
    '--lockfile-only',
    '--no-optional',
    '--prod',
    '--production',
    '--resolution-only',
    '--version',
    '-g',
    '-h',
    '-v',
  ]),
  ...pnpmOptions('neutral', 1, [
    '--filter',
    '--filter-prod',
    '--modules-dir',
    '-F',
  ]),
  ...pnpmOptions('limit', 0, [
    '--bail',
    '--fail-if-no-match',
    '--include-workspace-root',
    '--link-workspace-packages',
    '--no-bail',
    '--no-link-workspace-packages',
    '--no-shared-workspace-lockfile',
    '--no-sort',
    '--reverse',
    '--shared-workspace-lockfile',
    '--sort',
  ]),
  ...pnpmOptions('limit', 1, [
    '--changed-files-ignore-pattern',
    '--global-dir',
    '--test-pattern',
    '--workspace-concurrency',
  ]),
]);

const PNPM_COMMAND_OPTIONS = new Map([
  ['add', ADD_OPTIONS],
  ['i', INSTALL_OPTIONS],
  ['install', INSTALL_OPTIONS],
]);

const PNPM_OPTION_ARITIES = new Map();
for (const commandOptions of PNPM_COMMAND_OPTIONS.values()) {
  for (const [name, { arity }] of commandOptions) {
    const priorArity = PNPM_OPTION_ARITIES.get(name);
    if (priorArity !== undefined && priorArity !== arity)
      throw new Error(`pnpm option ${name} has inconsistent arity`);
    PNPM_OPTION_ARITIES.set(name, arity);
  }
}

for (const name of ['--dir', '--prefix', '-C']) {
  PNPM_OPTION_ARITIES.set(name, 1);
  INSTALL_OPTIONS.set(name, { arity: 1, behavior: 'directory' });
  ADD_OPTIONS.set(name, { arity: 1, behavior: 'directory' });
}

const SHORT_PNPM_OPTIONS = new Map(
  [...PNPM_OPTION_ARITIES].filter(([name]) => /^-[^-]$/u.test(name)),
);

const XARGS_OPTIONS = new Map([
  ['--arg-file', 1],
  ['--delimiter', 1],
  ['--exit', 0],
  ['--max-args', 1],
  ['--max-chars', 1],
  ['--max-procs', 1],
  ['--no-run-if-empty', 0],
  ['--null', 0],
  ['--replace', 1],
  ['--verbose', 0],
  ['-0', 0],
  ['-I', 1],
  ['-P', 1],
  ['-a', 1],
  ['-d', 1],
  ['-n', 1],
  ['-r', 0],
  ['-s', 1],
  ['-t', 0],
  ['-x', 0],
]);

const WRAPPERS = new Map([
  ['builtin', { options: new Map(), kind: 'ordinary' }],
  ['command', { options: new Map([['-p', 0]]), kind: 'command' }],
  [
    'env',
    {
      options: new Map([
        ['--ignore-environment', 0],
        ['--unset', 1],
        ['-i', 0],
        ['-u', 1],
      ]),
      assignments: true,
      kind: 'ordinary',
    },
  ],
  [
    'exec',
    {
      options: new Map([
        ['-c', 0],
        ['-l', 0],
      ]),
      kind: 'ordinary',
    },
  ],
  [
    'nice',
    {
      options: new Map([
        ['--adjustment', 1],
        ['-n', 1],
      ]),
      kind: 'ordinary',
    },
  ],
  ['nohup', { options: new Map(), kind: 'ordinary' }],
  [
    'stdbuf',
    {
      options: new Map([
        ['--error', 1],
        ['--input', 1],
        ['--output', 1],
        ['-e', 1],
        ['-i', 1],
        ['-o', 1],
      ]),
      kind: 'ordinary',
    },
  ],
  [
    'sudo',
    {
      options: new Map([
        ['--group', 1],
        ['--non-interactive', 0],
        ['--preserve-env', 0],
        ['--set-home', 0],
        ['--user', 1],
        ['-E', 0],
        ['-H', 0],
        ['-g', 1],
        ['-n', 0],
        ['-u', 1],
      ]),
      kind: 'ordinary',
    },
  ],
  [
    'time',
    {
      options: new Map([
        ['--portability', 0],
        ['-p', 0],
      ]),
      kind: 'ordinary',
    },
  ],
  [
    'timeout',
    {
      options: new Map([
        ['--foreground', 0],
        ['--kill-after', 1],
        ['--preserve-status', 0],
        ['--signal', 1],
        ['--verbose', 0],
        ['-k', 1],
        ['-s', 1],
      ]),
      prefixOperands: 1,
      kind: 'ordinary',
    },
  ],
  ['xargs', { options: XARGS_OPTIONS, kind: 'xargs' }],
]);

const DATA_EXECUTABLES = new Set([
  ':',
  '[',
  '[[',
  'echo',
  'false',
  'printf',
  'test',
  'true',
]);

const CODE_BEARING_EXECUTABLES = new Set([
  '.',
  'alias',
  'coproc',
  'eval',
  'source',
  'trap',
]);

const SHELL_EXECUTABLES = new Set(['bash', 'dash', 'sh', 'zsh']);

function literalValue(node) {
  if (!node) return undefined;
  if (node.text.includes('\\') || node.text.includes(GITHUB_EXPRESSION_MARKER))
    return undefined;
  if (node.type === 'number') return node.text;
  if (node.type === 'word')
    return /[\r\n]/u.test(node.text) ? undefined : node.text;
  if (node.type === 'raw_string') return node.text.slice(1, -1);
  if (node.type === 'string') {
    return node.namedChildren.every((child) => child.type === 'string_content')
      ? node.namedChildren.map((child) => child.text).join('')
      : undefined;
  }
  if (node.type === 'concatenation') {
    const pieces = node.namedChildren.map(literalValue);
    return pieces.every((piece) => piece !== undefined)
      ? pieces.join('')
      : undefined;
  }
  if (node.type === 'command_name') return literalValue(node.namedChild(0));
  return undefined;
}

function basenameOfExecutable(value) {
  return value.split('/').at(-1);
}

function commandTokens(node) {
  const nameNode = node.childForFieldName('name');
  if (!nameNode) return [];
  const tokens = [{ node: nameNode, value: literalValue(nameNode) }];
  for (let index = 0; index < node.childCount; index += 1) {
    const child = node.child(index);
    if (
      !child.isNamed ||
      child.id === nameNode.id ||
      node.fieldNameForChild(index) !== 'argument'
    ) {
      continue;
    }
    tokens.push({ node: child, value: literalValue(child) });
  }
  const firstArgument = tokens[1]?.node;
  if (
    firstArgument &&
    /^\\(?:\r\n|[\r\n])/u.test(
      node.text.slice(nameNode.endIndex - node.startIndex),
    )
  ) {
    tokens[0].value = undefined;
  }
  return tokens;
}

function inlineOption(token, option) {
  return token.startsWith(`${option}=`)
    ? token.slice(option.length + 1)
    : undefined;
}

function parseShortPnpmOptions(token) {
  if (!/^-[^-]+/u.test(token)) return undefined;
  const parsed = [];
  for (let index = 1; index < token.length; index += 1) {
    const name = `-${token[index]}`;
    const arity = SHORT_PNPM_OPTIONS.get(name);
    if (arity === undefined) return undefined;
    if (arity === 0) {
      parsed.push({ name });
      continue;
    }
    let value = token.slice(index + 1);
    if (value.startsWith('=')) value = value.slice(1);
    parsed.push({ name, value: value || undefined });
    return parsed;
  }
  return parsed;
}

function pnpmInvocation(tokens) {
  let optionsEnabled = true;
  let subcommand;
  let unresolved;
  let unresolvedBeforeSubcommand = false;
  const options = [];

  for (let index = 1; index < tokens.length; index += 1) {
    const token = tokens[index].value;
    if (token === undefined) {
      if (
        subcommand === undefined ||
        ['add', 'i', 'install'].includes(subcommand)
      )
        unresolved ??= 'non-literal pnpm argument';
      if (subcommand === undefined) unresolvedBeforeSubcommand = true;
      continue;
    }
    if (optionsEnabled && token === '--') {
      optionsEnabled = false;
      continue;
    }
    if (optionsEnabled && token.startsWith('--')) {
      const optionName = token.split('=', 1)[0];
      const arity = PNPM_OPTION_ARITIES.get(optionName);
      if (arity === undefined) {
        unresolved ??= `unmodelled pnpm option ${optionName}`;
        if (subcommand === undefined) {
          unresolvedBeforeSubcommand = true;
          break;
        }
        continue;
      }
      if (arity === 0) {
        if (token.includes('='))
          unresolved ??= `unmodelled pnpm option ${optionName}`;
        if (token.includes('=') && subcommand === undefined)
          unresolvedBeforeSubcommand = true;
        options.push({ name: optionName });
        continue;
      }
      const inline = inlineOption(token, optionName);
      const value = inline ?? tokens[index + 1]?.value;
      if (inline === undefined) index += 1;
      if (value === undefined)
        unresolved ??= `non-literal ${optionName} operand`;
      if (value === undefined && subcommand === undefined)
        unresolvedBeforeSubcommand = true;
      options.push({ name: optionName, value });
      continue;
    }
    if (optionsEnabled && token.startsWith('-') && token !== '-') {
      const combined = parseShortPnpmOptions(token);
      if (!combined) {
        unresolved ??= `unmodelled pnpm option ${token}`;
        if (subcommand === undefined) {
          unresolvedBeforeSubcommand = true;
          break;
        }
        continue;
      }
      for (const option of combined) {
        if (
          PNPM_OPTION_ARITIES.get(option.name) === 1 &&
          option.value === undefined
        ) {
          option.value = tokens[index + 1]?.value;
          index += 1;
          if (option.value === undefined)
            unresolved ??= `non-literal ${option.name} operand`;
          if (option.value === undefined && subcommand === undefined)
            unresolvedBeforeSubcommand = true;
        }
        options.push(option);
      }
      continue;
    }
    subcommand ??= token;
  }

  if (subcommand === undefined) {
    return unresolved
      ? { kind: 'limit', reason: unresolved }
      : { kind: 'neutral' };
  }
  if (unresolvedBeforeSubcommand) return { kind: 'limit', reason: unresolved };
  if (['create', 'dlx'].includes(subcommand)) return { kind: 'neutral' };
  if (!['add', 'i', 'install'].includes(subcommand)) {
    return { kind: 'consumer' };
  }
  if (unresolved) return { kind: 'limit', reason: unresolved };

  const commandOptions = PNPM_COMMAND_OPTIONS.get(subcommand);
  for (const option of options) {
    const policy = commandOptions.get(option.name);
    if (!policy)
      return {
        kind: 'limit',
        reason: `unmodelled pnpm option ${option.name} for ${subcommand}`,
      };
    if (policy.behavior === 'limit')
      return {
        kind: 'limit',
        reason: `pnpm ${subcommand} option ${option.name} is not establishing`,
      };
    if (
      policy.behavior === 'neutral' ||
      (policy.behavior === 'directory' && !isRepositoryRootPath(option.value))
    ) {
      return { kind: 'neutral-install' };
    }
  }
  return { kind: 'install' };
}

export function isRepositoryRootPath(value) {
  return typeof value === 'string' && /^\.(?:\/\.)*\/*$/u.test(value.trim());
}

export function classifyPnpmInstallArguments(args, { recursive = false } = {}) {
  if (
    !Array.isArray(args) ||
    args.some((argument) => typeof argument !== 'string')
  ) {
    return { kind: 'limit', reason: 'non-literal pnpm/action-setup args' };
  }
  const values = [
    'pnpm',
    ...(recursive ? ['--recursive'] : []),
    'install',
    ...args,
  ];
  return pnpmInvocation(values.map((value) => ({ value })));
}

function unwrap(tokens, specification) {
  let index = 1;
  let prefixOperands = specification.prefixOperands ?? 0;
  for (; index < tokens.length; index += 1) {
    const token = tokens[index].value;
    if (token === undefined) {
      if (prefixOperands > 0) return { limit: 'non-literal wrapper argument' };
      break;
    }
    if (token === '--') {
      index += 1;
      while (prefixOperands > 0) {
        if (tokens[index]?.value === undefined)
          return { limit: 'non-literal wrapper argument' };
        prefixOperands -= 1;
        index += 1;
      }
      break;
    }
    if (specification.assignments && /^[A-Za-z_][A-Za-z0-9_]*=/u.test(token)) {
      continue;
    }
    if (token.startsWith('-') && token !== '-') {
      let optionName = token.split('=', 1)[0];
      let arity = specification.options.get(optionName);
      let attachedValue;
      if (arity === undefined && /^-[^-].+/u.test(token)) {
        optionName = token.slice(0, 2);
        arity = specification.options.get(optionName);
        attachedValue = token.slice(2);
      }
      if (arity === undefined)
        return { limit: `unmodelled wrapper option ${optionName}` };
      if (
        arity === 1 &&
        attachedValue === undefined &&
        inlineOption(token, optionName) === undefined
      ) {
        if (tokens[index + 1]?.value === undefined)
          return { limit: `non-literal ${optionName} operand` };
        index += 1;
      }
      continue;
    }
    if (prefixOperands > 0) {
      prefixOperands -= 1;
      continue;
    }
    break;
  }
  if (prefixOperands > 0) return { limit: 'missing wrapper operand' };
  return { tokens: tokens.slice(index) };
}

function literalPnpmArgument(tokens) {
  return tokens.slice(1).some(({ value }) => {
    if (typeof value !== 'string') return false;
    return basenameOfExecutable(value) === 'pnpm';
  });
}

function shellCarriesCode(tokens, stdinWritten) {
  if (stdinWritten) return true;
  for (const { node, value } of tokens.slice(1)) {
    if (node.type === 'process_substitution') return true;
    if (value === undefined) return true;
    if (value === '--') continue;
    if (/^-[^-]*[cs]/u.test(value) || ['--command', '--stdin'].includes(value))
      return true;
  }
  return false;
}

function setOnlyEnablesOptions(tokens) {
  if (tokens.length === 1) return true;
  for (let index = 1; index < tokens.length; index += 1) {
    const token = tokens[index].value;
    if (typeof token !== 'string' || token.startsWith('+') || token === '--')
      return false;
    if (token === '-o' || /^-[eux]+o$/u.test(token)) {
      const optionName = tokens[index + 1]?.value;
      if (!['errexit', 'nounset', 'pipefail'].includes(optionName))
        return false;
      index += 1;
      continue;
    }
    if (!/^-[eux]+$/u.test(token)) return false;
  }
  return true;
}

function classifyCommand(tokens, commandContext = {}) {
  const { stdinWritten = false } = commandContext;
  const executableValue = tokens[0]?.value;
  if (executableValue === undefined)
    return { kind: 'limit', reason: 'non-literal executable' };
  const executable = basenameOfExecutable(executableValue);
  if (executable === 'pnpm') return pnpmInvocation(tokens);

  const wrapper = WRAPPERS.get(executable);
  if (wrapper) {
    if (wrapper.kind === 'command' && ['-v', '-V'].includes(tokens[1]?.value)) {
      return { kind: 'neutral' };
    }
    const unwrapped = unwrap(tokens, wrapper);
    if (unwrapped.limit) return { kind: 'limit', reason: unwrapped.limit };
    if (unwrapped.tokens.length === 0) return { kind: 'neutral' };
    const classification = classifyCommand(unwrapped.tokens, commandContext);
    if (wrapper.kind === 'xargs' && classification.kind === 'install') {
      return { kind: 'neutral-install' };
    }
    return classification;
  }

  if (CODE_BEARING_EXECUTABLES.has(executable))
    return { kind: 'limit', reason: `code-bearing ${executable}` };
  if (SHELL_EXECUTABLES.has(executable)) {
    return shellCarriesCode(tokens, stdinWritten)
      ? { kind: 'limit', reason: `code-bearing ${executable}` }
      : { kind: 'neutral' };
  }
  if (DATA_EXECUTABLES.has(executable)) {
    if ([':', 'true'].includes(executable))
      return { kind: 'neutral', status: 'success' };
    if (executable === 'false') return { kind: 'neutral', status: 'failure' };
    return { kind: 'neutral' };
  }
  if (['type', 'which'].includes(executable)) return { kind: 'neutral' };
  if (
    executable === 'hash' &&
    !tokens.slice(1).some(({ value }) => value === '-p')
  )
    return { kind: 'neutral' };
  if (
    executable === 'corepack' &&
    ['enable', 'install', 'prepare', 'use'].includes(tokens[1]?.value)
  ) {
    return { kind: 'neutral' };
  }
  if (executable === 'npm' && ['i', 'install'].includes(tokens[1]?.value))
    return { kind: 'neutral' };
  if (literalPnpmArgument(tokens))
    return { kind: 'limit', reason: `unmodelled ${executable} invocation` };
  if (['cd', 'popd', 'pushd'].includes(executable))
    return { kind: 'state', mutation: 'directory' };
  if (executable === 'set') {
    return setOnlyEnablesOptions(tokens)
      ? { kind: 'neutral' }
      : { kind: 'state', mutation: 'shell-options' };
  }
  if (executable === 'shopt')
    return { kind: 'state', mutation: 'shell-options' };
  return { kind: 'neutral' };
}

function copyState(state, changes) {
  return { ...state, ...changes };
}

function intersectStates(left, right) {
  return {
    directoryChanged: left.directoryChanged || right.directoryChanged,
    installed: left.installed && right.installed,
    newInstall: left.newInstall && right.newInstall,
    shellOptionsTouched: left.shellOptionsTouched || right.shellOptionsTouched,
    suppressLimits: left.suppressLimits && right.suppressLimits,
  };
}

function carryTaints(state, ...outcomes) {
  const states = outcomes.flatMap((outcome) => [
    outcome.failure,
    outcome.success,
  ]);
  return copyState(state, {
    directoryChanged:
      state.directoryChanged ||
      states.some((candidate) => candidate.directoryChanged),
    shellOptionsTouched:
      state.shellOptionsTouched ||
      states.some((candidate) => candidate.shellOptionsTouched),
  });
}

function issue(context, kind, node, reason) {
  context.issues.push({
    kind,
    command: context.source.slice(node.startIndex, node.endIndex),
    index: node.startIndex,
    ...(reason ? { reason } : {}),
  });
}

function recordLimit(node, state, context, reason) {
  context.usesPnpm = true;
  if (!state.suppressLimits) {
    issue(context, 'limit', node, reason);
    context.stopped = true;
  }
}

function effectiveInstallMode(state, context, structuralMode) {
  if (
    structuralMode === 'limit' ||
    context.installPolicy === 'limit' ||
    state.directoryChanged ||
    state.shellOptionsTouched
  ) {
    return 'limit';
  }
  return 'establish';
}

function stdinRedirect(node) {
  for (const child of node.children) {
    if (['<', '<<', '<<<', '<&', '<>'].includes(child.type)) return true;
  }
  return false;
}

function commandChildren(node) {
  const name = node.childForFieldName('name');
  return node.children.filter(
    (child) => child.isNamed && (!name || child.id !== name.id),
  );
}

function analyzeCommand(node, state, context, mode, metadata = {}) {
  const commandContext = {
    ...metadata,
    mode,
    stdinWritten:
      metadata.stdinWritten ||
      node.children.some(
        (child) => child.type.endsWith('_redirect') && stdinRedirect(child),
      ),
  };
  const classification = classifyCommand(commandTokens(node), commandContext);
  if (classification.kind !== 'neutral' && classification.kind !== 'state')
    context.usesPnpm = true;
  if (['install', 'neutral-install'].includes(classification.kind))
    context.installsPnpm = true;
  if (classification.kind === 'limit') {
    recordLimit(node, state, context, classification.reason);
    return {
      canFail: true,
      canSucceed: true,
      failure: state,
      success: state,
    };
  }
  let childState = state;
  for (const child of commandChildren(node)) {
    if (context.stopped) break;
    childState = analyzeNode(child, childState, context, mode).success;
  }
  if (context.stopped)
    return {
      canFail: true,
      canSucceed: true,
      failure: state,
      success: childState,
    };

  if (classification.kind === 'consumer' && !childState.installed)
    issue(context, 'missing', node);

  if (classification.kind === 'install') {
    const behavior = effectiveInstallMode(childState, context, mode);
    if (behavior === 'limit') {
      recordLimit(node, childState, context, 'install is not guaranteed');
      return {
        canFail: true,
        canSucceed: true,
        failure: state,
        success: childState,
      };
    }
    if (behavior === 'establish') {
      return {
        canFail: true,
        canSucceed: true,
        failure: state,
        success: copyState(childState, {
          installed: true,
          newInstall: true,
          suppressLimits: true,
        }),
      };
    }
  }
  if (classification.kind === 'state') {
    const changes =
      classification.mutation === 'directory'
        ? { directoryChanged: true }
        : { shellOptionsTouched: true };
    return {
      canFail: true,
      canSucceed: true,
      failure: state,
      success: copyState(childState, changes),
    };
  }
  return {
    canFail: classification.status !== 'success',
    canSucceed: classification.status !== 'failure',
    failure: state,
    success: childState,
  };
}

function namedChildren(children) {
  return children.filter((child) => child.isNamed);
}

const SEQUENCE_SEPARATORS = new Set([
  '&',
  ';',
  ';;',
  '{',
  '}',
  'done',
  'elif',
  'else',
  'fi',
]);

function analyzeSequence(children, state, context, mode) {
  let current = state;
  let failure = state;
  const statements = namedChildren(children);
  for (const [index, child] of statements.entries()) {
    if (context.stopped) break;
    const childIndex = children.findIndex(
      (candidate) => candidate.id === child.id,
    );
    const nextNamedIndex = children.findIndex(
      (candidate, candidateIndex) =>
        candidateIndex > childIndex && candidate.isNamed,
    );
    const following = children.slice(
      childIndex + 1,
      nextNamedIndex === -1 ? children.length : nextNamedIndex,
    );
    const unknownSeparator = following.find(
      (candidate) =>
        !candidate.isNamed && !SEQUENCE_SEPARATORS.has(candidate.type),
    );
    const backgrounded = following.some((candidate) => candidate.type === '&');
    const childMode = backgrounded || unknownSeparator ? 'limit' : mode;
    const outcome = analyzeNode(child, current, context, childMode, {
      listFinal: index === statements.length - 1,
    });
    failure = intersectStates(failure, outcome.failure);
    current = backgrounded ? current : outcome.success;
  }
  return { failure, success: current };
}

function analyzeList(node, state, context, mode, metadata) {
  const children = node.children;
  const operands = children.filter((child) => child.isNamed);
  if (operands.length === 0) return { failure: state, success: state };
  const listMode = metadata.listFinal ? mode : 'limit';
  let outcome = analyzeNode(operands[0], state, context, listMode, {
    listFinal: true,
  });
  for (let index = 1; index < operands.length && !context.stopped; index += 1) {
    const left = operands[index - 1];
    const right = operands[index];
    const operator = children.find(
      (child) =>
        !child.isNamed &&
        child.startIndex >= left.endIndex &&
        child.endIndex <= right.startIndex &&
        ['&&', '||'].includes(child.type),
    );
    if (!operator) {
      recordLimit(node, state, context, 'unmodelled list operator');
      return outcome;
    }
    if (operator.type === '&&') {
      if (outcome.canSucceed === false) continue;
      const rightOutcome = analyzeNode(
        right,
        outcome.success,
        context,
        listMode,
        {
          listFinal: true,
        },
      );
      outcome = {
        canFail: outcome.canFail !== false || rightOutcome.canFail !== false,
        canSucceed: rightOutcome.canSucceed !== false,
        failure:
          outcome.canFail === false
            ? rightOutcome.failure
            : rightOutcome.canFail === false
              ? outcome.failure
              : intersectStates(outcome.failure, rightOutcome.failure),
        success: rightOutcome.success,
      };
    } else {
      if (outcome.canFail === false) continue;
      const rightOutcome = analyzeNode(
        right,
        outcome.failure,
        context,
        listMode,
        {
          listFinal: true,
        },
      );
      outcome = {
        canFail: rightOutcome.canFail !== false,
        canSucceed:
          outcome.canSucceed !== false || rightOutcome.canSucceed !== false,
        failure: rightOutcome.failure,
        success:
          outcome.canSucceed === false
            ? rightOutcome.success
            : rightOutcome.canSucceed === false
              ? outcome.success
              : intersectStates(outcome.success, rightOutcome.success),
      };
    }
  }
  return outcome;
}

function fieldChildren(node, fieldName) {
  const children = [];
  for (let index = 0; index < node.childCount; index += 1) {
    const child = node.child(index);
    if (child.isNamed && node.fieldNameForChild(index) === fieldName)
      children.push(child);
  }
  return children;
}

function childrenAfter(node, tokenType) {
  const tokenIndex = node.children.findIndex(
    (child) => child.type === tokenType,
  );
  return tokenIndex === -1 ? [] : node.children.slice(tokenIndex + 1);
}

function analyzeIf(node, state, context, mode) {
  const conditions = fieldChildren(node, 'condition');
  const conditionOutcome = analyzeSequence(conditions, state, context, 'limit');
  if (context.stopped) return { failure: state, success: state };
  const clauses = node.namedChildren.filter((child) =>
    ['elif_clause', 'else_clause'].includes(child.type),
  );
  const clauseIds = new Set(clauses.map((child) => child.id));
  const conditionIds = new Set(conditions.map((child) => child.id));
  const thenToken = node.children.find(
    (candidate) => candidate.type === 'then',
  );
  const consequent = node.children.filter(
    (child) =>
      !conditionIds.has(child.id) &&
      !clauseIds.has(child.id) &&
      child.startIndex > (thenToken?.endIndex ?? node.startIndex) &&
      child.endIndex < (clauses[0]?.startIndex ?? node.endIndex),
  );
  const branches = [
    analyzeSequence(consequent, conditionOutcome.success, context, mode)
      .success,
  ];
  let unmatched = conditionOutcome.failure;
  let hasElse = false;
  for (const clause of clauses) {
    if (context.stopped) break;
    if (clause.type === 'else_clause') {
      hasElse = true;
      branches.push(
        analyzeSequence(childrenAfter(clause, 'else'), unmatched, context, mode)
          .success,
      );
      continue;
    }
    const clauseChildren = clause.children;
    const thenIndex = clauseChildren.findIndex(
      (child) => child.type === 'then',
    );
    const elifCondition = clauseChildren.slice(1, thenIndex);
    const elifOutcome = analyzeSequence(
      elifCondition,
      unmatched,
      context,
      'limit',
    );
    branches.push(
      analyzeSequence(
        clauseChildren.slice(thenIndex + 1),
        elifOutcome.success,
        context,
        mode,
      ).success,
    );
    unmatched = elifOutcome.failure;
  }
  if (!hasElse) branches.push(unmatched);
  return {
    failure: state,
    success: branches.reduce(intersectStates),
  };
}

function analyzeCase(node, state, context, mode) {
  let selectorState = state;
  for (const selector of fieldChildren(node, 'value')) {
    selectorState = analyzeNode(
      selector,
      selectorState,
      context,
      'limit',
    ).success;
    if (context.stopped) return { failure: state, success: state };
  }
  const items = node.namedChildren.filter(
    (child) => child.type === 'case_item',
  );
  if (items.some((item) => item.childForFieldName('fallthrough'))) {
    recordLimit(node, state, context, 'case fallthrough');
    return { failure: state, success: state };
  }
  const branches = [];
  let hasWildcard = false;
  for (const item of items) {
    const patterns = fieldChildren(item, 'value');
    let branchState = selectorState;
    for (const pattern of patterns) {
      if (pattern.text === '*') hasWildcard = true;
      branchState = analyzeNode(pattern, branchState, context, 'limit').success;
      if (context.stopped) return { failure: state, success: state };
    }
    const closeIndex = item.children.findIndex((child) => child.type === ')');
    const body = item.children.slice(closeIndex + 1);
    branches.push(analyzeSequence(body, branchState, context, mode).success);
  }
  if (!hasWildcard) branches.push(selectorState);
  return {
    failure: state,
    success: branches.reduce(intersectStates),
  };
}

function analyzeNonEstablishingChildren(
  node,
  state,
  context,
  mode,
  retainTaints,
) {
  let current = state;
  const outcomes = [];
  for (const child of node.children) {
    if (!child.isNamed) continue;
    const outcome = analyzeNode(child, current, context, mode);
    outcomes.push(outcome);
    current = outcome.success;
    if (context.stopped) break;
  }
  const result = retainTaints ? carryTaints(state, ...outcomes) : state;
  return { failure: result, success: result };
}

function analyzeCurrentShellNonEstablishing(node, state, context) {
  return analyzeNonEstablishingChildren(node, state, context, 'limit', true);
}

function analyzeChildShell(node, state, context) {
  return analyzeNonEstablishingChildren(node, state, context, 'limit', false);
}

function analyzeRedirected(node, state, context, mode) {
  const body = node.childForFieldName('body');
  let current = state;
  const redirects = fieldChildren(node, 'redirect');
  const stdinWritten = redirects.some(stdinRedirect);
  if (
    body?.type === 'command' &&
    SHELL_EXECUTABLES.has(
      basenameOfExecutable(commandTokens(body)[0]?.value ?? ''),
    ) &&
    stdinWritten
  ) {
    return analyzeNode(body, current, context, mode, { stdinWritten });
  }
  for (const redirect of redirects) {
    current = analyzeNode(redirect, current, context, mode).success;
    if (context.stopped) return { failure: state, success: current };
  }
  if (!body) return { failure: state, success: current };
  return analyzeNode(body, current, context, mode, { stdinWritten });
}

function analyzePipeline(node, state, context) {
  const commands = node.children.filter((child) => child.isNamed);
  for (const [index, command] of commands.entries()) {
    analyzeNode(command, state, context, 'limit', {
      stdinWritten: index > 0,
    });
    if (context.stopped) break;
  }
  return { failure: state, success: state };
}

function analyzeOpaque(node, state, context) {
  recordLimit(node, state, context, `code-bearing ${node.type}`);
  return { failure: state, success: state };
}

function analyzeExpressionContainer(node, state, context, mode) {
  return analyzeNonEstablishingChildren(node, state, context, mode, true);
}

function analyzeStatementContainer(node, state, context, mode) {
  return analyzeSequence(node.children, state, context, mode);
}

function analyzeError(node, state, context) {
  recordLimit(node, state, context, 'Bash parse error');
  return { failure: state, success: state };
}

const NODE_HANDLERS = new Map([
  ['arithmetic_expansion', analyzeExpressionContainer],
  ['array', analyzeExpressionContainer],
  ['binary_expression', analyzeExpressionContainer],
  ['brace_expression', analyzeExpressionContainer],
  ['c_style_for_statement', analyzeCurrentShellNonEstablishing],
  ['case_item', analyzeStatementContainer],
  ['case_statement', analyzeCase],
  ['command', analyzeCommand],
  ['command_name', analyzeExpressionContainer],
  ['command_substitution', analyzeChildShell],
  ['compound_statement', analyzeStatementContainer],
  ['concatenation', analyzeExpressionContainer],
  ['declaration_command', analyzeExpressionContainer],
  ['do_group', analyzeStatementContainer],
  ['elif_clause', analyzeStatementContainer],
  ['else_clause', analyzeStatementContainer],
  ['expansion', analyzeExpressionContainer],
  ['file_redirect', analyzeExpressionContainer],
  ['for_statement', analyzeCurrentShellNonEstablishing],
  ['function_definition', analyzeOpaque],
  ['heredoc_body', analyzeExpressionContainer],
  ['heredoc_redirect', analyzeExpressionContainer],
  ['herestring_redirect', analyzeExpressionContainer],
  ['if_statement', analyzeIf],
  ['list', analyzeList],
  ['negated_command', analyzeCurrentShellNonEstablishing],
  ['number', analyzeExpressionContainer],
  ['parenthesized_expression', analyzeExpressionContainer],
  ['pipeline', analyzePipeline],
  ['postfix_expression', analyzeExpressionContainer],
  ['process_substitution', analyzeChildShell],
  ['program', analyzeStatementContainer],
  ['redirected_statement', analyzeRedirected],
  ['string', analyzeExpressionContainer],
  ['subscript', analyzeExpressionContainer],
  ['subshell', analyzeChildShell],
  ['ternary_expression', analyzeExpressionContainer],
  ['test_command', analyzeExpressionContainer],
  ['translated_string', analyzeExpressionContainer],
  ['unary_expression', analyzeExpressionContainer],
  ['unset_command', analyzeExpressionContainer],
  ['variable_assignment', analyzeExpressionContainer],
  ['variable_assignments', analyzeExpressionContainer],
  ['while_statement', analyzeCurrentShellNonEstablishing],
]);

const MODELLED_CONTAINER_TYPES = new Set(NODE_HANDLERS.keys());

function analyzeNode(node, state, context, mode = 'normal', metadata = {}) {
  if (context.stopped) return { failure: state, success: state };
  if (!node.isNamed) return { failure: state, success: state };
  if (node.type === 'ERROR') return analyzeError(node, state, context);
  const handler = NODE_HANDLERS.get(node.type);
  if (handler) return handler(node, state, context, mode, metadata);
  if (node.namedChildCount > 0) {
    for (const child of node.children) {
      if (!child.isNamed) continue;
      if (MODELLED_CONTAINER_TYPES.has(child.type)) {
        recordLimit(node, state, context, `unmodelled ${node.type} container`);
        break;
      }
    }
  }
  return { failure: state, success: state };
}

export function analyzePnpmCommands(
  source,
  { establishment = 'none', installPolicy = 'establish' } = {},
) {
  const parser = new Parser();
  parser.setLanguage(bashLanguage);
  const parseSource = source.replace(/\$\{\{[^\r\n]*?\}\}/gu, (expression) =>
    GITHUB_EXPRESSION_MARKER.repeat(expression.length),
  );
  const tree = parser.parse(parseSource);
  try {
    const context = {
      installPolicy,
      installsPnpm: false,
      issues: [],
      source,
      stopped: false,
      usesPnpm: false,
    };
    const installed = establishment !== 'none';
    const outcome = analyzeNode(
      tree.rootNode,
      {
        directoryChanged: false,
        installed,
        newInstall: false,
        shellOptionsTouched: false,
        suppressLimits: establishment === 'unconditional',
      },
      context,
    );
    return {
      establishesInstall: outcome.success.newInstall,
      installsPnpm: context.installsPnpm,
      issues: context.issues.sort((left, right) => left.index - right.index),
      usesPnpm: context.usesPnpm,
    };
  } finally {
    tree.delete();
    parser.delete();
  }
}
