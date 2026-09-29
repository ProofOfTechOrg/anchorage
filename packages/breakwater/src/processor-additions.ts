// SPDX-License-Identifier: Apache-2.0
// The prompt messages a guarded agent's application input processors add or
// change on a call's message list, recorded for the policy engine.
//
// Mastra renders every system message and every message outside the call's
// input into the prompt, and the input policies read the input. A processor
// that moves the caller's text there would give it to the model unread, so
// each processor's call is compared with the list as it stood before, and a
// copy of each message it added or changed is kept, keyed by the list, until
// the policy engine takes it.
//
// @internal

import {
  type MastraDBMessage,
  MessageList,
} from '@mastra/core/agent/message-list';

/** @internal A text part of a recorded system message. */
export interface RecordedTextPart {
  readonly text: string;
  readonly providerOptions: unknown;
}

/**
 * @internal A recorded system message: its content, and the provider options
 * Mastra sends with it.
 */
export interface RecordedSystemMessage {
  readonly content: string | readonly RecordedTextPart[];
  readonly providerOptions: unknown;
  readonly experimental_providerMetadata: unknown;
}

/** @internal A prompt message an application input processor added or changed. */
export type ProcessorAddition =
  | { readonly kind: 'system'; readonly message: RecordedSystemMessage }
  | { readonly kind: 'message'; readonly message: MastraDBMessage };

/** @internal The prompt messages of a list, counted by fingerprint. */
export type PromptSnapshot = ReadonlyMap<string, number>;

const additionsByList = new WeakMap<MessageList, ProcessorAddition[]>();

const identities = new WeakMap<object, number>();
let lastIdentity = 0;

function identityOf(value: object): number {
  const known = identities.get(value);
  if (known !== undefined) return known;
  lastIdentity += 1;
  identities.set(value, lastIdentity);
  return lastIdentity;
}

// Text that differs when any value reachable through the named own
// properties differs, a data property turned into an accessor included. It
// reads property descriptors, so it never runs a getter, and it classifies
// nothing: an entry present before a processor runs is never refused. An own
// object property whose value is `undefined` reads as a missing one, as
// Mastra renders the two alike, so a processor that copies a message without
// changing what the model receives changes nothing here; an array element
// keeps its index.
function fingerprint(
  value: unknown,
  keys?: readonly string[],
  ancestors: readonly object[] = [],
): string {
  if (typeof value === 'string') return JSON.stringify(value);
  if (typeof value === 'bigint') return `${value}n`;
  if (typeof value === 'function') return `function#${identityOf(value)}`;
  if (typeof value !== 'object' || value === null) return String(value);
  if (ancestors.includes(value)) return 'cycle';
  const nested = [...ancestors, value];
  const fields: string[] = [];
  for (const key of keys ?? Object.keys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    let field: string;
    if (descriptor === undefined) field = 'absent';
    else if (!('value' in descriptor)) {
      const getter = descriptor.get ? identityOf(descriptor.get) : 0;
      const setter = descriptor.set ? identityOf(descriptor.set) : 0;
      field = `accessor#${getter}/${setter}`;
    } else if (descriptor.value !== undefined || Array.isArray(value)) {
      field = fingerprint(descriptor.value, undefined, nested);
    } else if (keys === undefined) continue;
    else field = 'absent';
    fields.push(`${JSON.stringify(key)}:${field}`);
  }
  return `${Array.isArray(value) ? 'array' : 'object'}{${fields.join(',')}}`;
}

const SYSTEM_FIELDS = [
  'content',
  'providerOptions',
  'experimental_providerMetadata',
] as const;
const MESSAGE_FIELDS = ['id', 'role', 'content'] as const;

interface PromptEntries {
  readonly system: readonly unknown[];
  readonly messages: readonly MastraDBMessage[];
}

/**
 * @internal The ids of the messages memory holds on `messageList`. A message
 * whose id is among them is not the call's input, even while the list holds
 * it as input: Mastra keeps the id when a processor replaces a remembered
 * message, or merges into one, with source `input`. Read through the
 * prototype, so an instance override cannot make this record and the policy
 * engine disagree on what the input is.
 */
export function rememberedIds(messageList: MessageList): ReadonlySet<string> {
  return MessageList.prototype.makeMessageSourceChecker.call(messageList)
    .memory;
}

// Every system message, untagged and tagged, read through the prototype as
// Mastra's rendering reads its fields, and every message that is not the
// call's input.
function promptEntries(messageList: MessageList): PromptEntries {
  const remembered = rememberedIds(messageList);
  const input = new Set(
    messageList.get.input
      .db()
      .flatMap(({ id }) => (remembered.has(id) ? [] : [id])),
  );
  return {
    system: MessageList.prototype.getAllSystemMessages.call(messageList),
    messages: messageList.get.all.db().filter(({ id }) => !input.has(id)),
  };
}

const systemKey = (entry: unknown) =>
  `system:${fingerprint(entry, SYSTEM_FIELDS)}`;
const messageKey = (message: MastraDBMessage) =>
  `message:${fingerprint(message, MESSAGE_FIELDS)}`;

/** @internal The ids of the messages among `additions`. */
export function addedMessageIds(
  additions: readonly ProcessorAddition[],
): string[] {
  return additions.flatMap((addition) =>
    addition.kind === 'message' ? [addition.message.id] : [],
  );
}

/**
 * @internal Count the system messages and the messages outside the call's
 * input on `messageList` by fingerprint, before an application input
 * processor runs.
 */
export function snapshotPromptMessages(
  messageList: MessageList,
): PromptSnapshot {
  const counts = new Map<string, number>();
  const { system, messages } = promptEntries(messageList);
  for (const key of [...system.map(systemKey), ...messages.map(messageKey)]) {
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return counts;
}

function takeOne(counts: Map<string, number>, key: string): boolean {
  const count = counts.get(key) ?? 0;
  if (count === 0) return false;
  counts.set(key, count - 1);
  return true;
}

const UNREADABLE_SYSTEM_MESSAGE =
  'a system message an application input processor added or changed is not text';

// An own data property's value, or undefined when there is none. An accessor
// could show this check one value and Mastra's rendering another.
function ownData(value: object, key: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  if (descriptor === undefined) return undefined;
  if (!('value' in descriptor)) throw new TypeError(UNREADABLE_SYSTEM_MESSAGE);
  return descriptor.value;
}

function isPlainObject(value: unknown): value is object {
  if (typeof value !== 'object' || value === null) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

// Mastra sends a system message's text parts joined and drops its other
// parts, so a message with any other content is refused rather than read in
// part.
function readTextParts(content: unknown): readonly RecordedTextPart[] {
  if (!Array.isArray(content)) throw new TypeError(UNREADABLE_SYSTEM_MESSAGE);
  return content.map((part: unknown) => {
    if (!isPlainObject(part) || ownData(part, 'type') !== 'text') {
      throw new TypeError(UNREADABLE_SYSTEM_MESSAGE);
    }
    const text = ownData(part, 'text');
    if (typeof text !== 'string') {
      throw new TypeError(UNREADABLE_SYSTEM_MESSAGE);
    }
    return {
      text,
      providerOptions: structuredClone(ownData(part, 'providerOptions')),
    };
  });
}

function readSystemMessage(entry: unknown): RecordedSystemMessage {
  if (typeof entry !== 'object' || entry === null) {
    throw new TypeError(UNREADABLE_SYSTEM_MESSAGE);
  }
  const content = ownData(entry, 'content');
  return {
    content: typeof content === 'string' ? content : readTextParts(content),
    providerOptions: structuredClone(ownData(entry, 'providerOptions')),
    experimental_providerMetadata: structuredClone(
      ownData(entry, 'experimental_providerMetadata'),
    ),
  };
}

/**
 * @internal Record a copy of each system message and each message outside
 * the call's input that is not in `before`, which an application input
 * processor therefore added or changed.
 *
 * @throws TypeError when such a system message's content is not a string or
 * an array of text parts, or when one of its fields is an accessor. A copy
 * that `structuredClone` refuses throws its `DataCloneError`.
 */
export function recordProcessorAdditions(
  messageList: MessageList,
  before: PromptSnapshot,
): void {
  const unmatched = new Map(before);
  const { system, messages } = promptEntries(messageList);
  const additions: ProcessorAddition[] = [];
  for (const entry of system) {
    if (takeOne(unmatched, systemKey(entry))) continue;
    additions.push({ kind: 'system', message: readSystemMessage(entry) });
  }
  for (const message of messages) {
    if (takeOne(unmatched, messageKey(message))) continue;
    additions.push({ kind: 'message', message: structuredClone(message) });
  }
  if (additions.length === 0) return;
  additionsByList.set(messageList, [
    ...(additionsByList.get(messageList) ?? []),
    ...additions,
  ]);
}

/**
 * @internal Remove and return what application input processors added or
 * changed on `messageList`, so a second evaluation of the list does not read
 * it again.
 */
export function takeProcessorAdditions(
  messageList: MessageList,
): readonly ProcessorAddition[] {
  const additions = additionsByList.get(messageList) ?? [];
  additionsByList.delete(messageList);
  return additions;
}

/**
 * @internal The additions whose text the policy engine reads beside
 * `messages`, the caller messages it evaluates: each system addition, and
 * each message addition unless `messages` holds one with the same id and the
 * same fingerprint, which is read there. Only the entries of `messages` whose
 * id an addition carries are fingerprinted.
 */
export function additionsToRead(
  additions: readonly ProcessorAddition[],
  messages: readonly MastraDBMessage[],
): readonly ProcessorAddition[] {
  const ids = new Set(addedMessageIds(additions));
  if (ids.size === 0) return additions;
  const read = new Set(
    messages.filter(({ id }) => ids.has(id)).map(messageKey),
  );
  return additions.filter(
    (addition) =>
      addition.kind === 'system' || !read.has(messageKey(addition.message)),
  );
}

/**
 * @internal The ids of the messages on `messageList` that `snapshot` does not
 * match, which a processor therefore added or changed since the snapshot was
 * taken. It compares fingerprints alone, with no copy and no classification,
 * so a value that stops a call does not stop its removal.
 */
export function unmatchedMessageIds(
  messageList: MessageList,
  snapshot: PromptSnapshot,
): string[] {
  const unmatched = new Map(snapshot);
  return messageList.get.all
    .db()
    .flatMap((message) =>
      takeOne(unmatched, messageKey(message)) ? [] : [message.id],
    );
}
