import { readFileSync, statSync } from 'node:fs';

const initialFence = {
  state: 'migration-locked',
  mutationEpoch: 0,
  requireMutationEpoch: false,
  transitionRevision: 0,
};

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function exactFence(value, expected) {
  assert(value && typeof value === 'object', 'fence reading is missing');
  for (const [key, expectedValue] of Object.entries(expected))
    assert(value[key] === expectedValue, `fence ${key} differs`);
  assert(value.proofKey === undefined, 'fence has a proof key');
  assert(value.proofRunId === undefined, 'fence has a proof run');
}

function nextFence(value, state) {
  assert(Number.isSafeInteger(value.mutationEpoch), 'invalid fence epoch');
  assert(
    Number.isSafeInteger(value.transitionRevision),
    'invalid fence revision',
  );
  assert(
    value.mutationEpoch < Number.MAX_SAFE_INTEGER,
    'fence epoch exhausted',
  );
  assert(
    value.transitionRevision < Number.MAX_SAFE_INTEGER,
    'fence revision exhausted',
  );
  return {
    state,
    mutationEpoch: value.mutationEpoch + 1,
    requireMutationEpoch: true,
    transitionRevision: value.transitionRevision + 1,
  };
}

export function readSecretFile(path) {
  assert(
    typeof path === 'string' && path.startsWith('/'),
    'absolute secret path required',
  );
  const info = statSync(path);
  assert(
    info.isFile() && (info.mode & 0o077) === 0,
    'secret file must be private',
  );
  const secret = readFileSync(path, 'utf8').trim();
  assert(/^[\x21-\x7e]{32,256}$/.test(secret), 'invalid probe secret');
  return secret;
}

export function localProbe({ port, secret, fetcher = fetch }) {
  assert(
    Number.isInteger(port) && port > 0 && port < 65536,
    'invalid local port',
  );
  return async (method, path, body) => {
    const response = await fetcher(`http://127.0.0.1:${port}${path}`, {
      method,
      headers: {
        authorization: `Bearer ${secret}`,
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.timeout(90_000),
    });
    assert(
      response.status === 200,
      `local probe ${path} returned ${response.status}`,
    );
    return response.json();
  };
}

export function trustedD1Checks(query) {
  let baseline;
  const counts = async () => {
    const rows = await query(
      "SELECT (SELECT COUNT(*) FROM mastra_notifications WHERE source = 'flowsafe-staging' AND kind = 'host-owned-notification') AS notifications, (SELECT COUNT(*) FROM flowsafe_notification_operation_receipts) AS receipts",
    );
    assert(
      rows.length === 1 &&
        Number.isSafeInteger(rows[0].notifications) &&
        Number.isSafeInteger(rows[0].receipts),
      'notification baseline is unreadable',
    );
    return rows[0];
  };
  return {
    preflightD1: async () => {
      const schema = await query(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'flowsafe_remote_step_invocations'",
      );
      assert(
        schema.length === 1 &&
          typeof schema[0].sql === 'string' &&
          schema[0].sql.includes('write_generation INTEGER NOT NULL') &&
          schema[0].sql.includes('operation_state TEXT'),
        'remote step v5 schema is missing',
      );
      const rows = await query(
        'SELECT COUNT(*) AS count FROM flowsafe_remote_step_invocations',
      );
      assert(
        rows.length === 1 && rows[0].count === 0,
        'remote step journal is not empty',
      );
      baseline = await counts();
    },
    verifyD1: async (invocationId) => {
      assert(
        /^(?!\.{1,2}$)[A-Za-z0-9._~-]{1,200}$/.test(invocationId),
        'invalid invocation ID',
      );
      const rows = await query(
        `SELECT i.status, i.settlement_outcome, i.operation_state, i.thread_id, i.notification_id, ` +
          `r.operation_key AS receipt_key, r.request_digest AS receipt_digest, ` +
          `r.thread_id AS receipt_thread_id, r.notification_id AS receipt_notification_id, ` +
          `n.id AS stored_notification_id, ` +
          `n.source, n.kind FROM flowsafe_remote_step_invocations i ` +
          `LEFT JOIN flowsafe_notification_operation_receipts r ON r.operation_key = i.operation_key ` +
          `AND r.thread_id = i.thread_id AND r.notification_id = i.notification_id ` +
          `AND r.request_digest = i.notification_request_digest ` +
          `LEFT JOIN mastra_notifications n ON n.id = i.notification_id AND n.thread_id = i.thread_id ` +
          `WHERE i.invocation_id = '${invocationId}'`,
      );
      assert(rows.length === 1, 'expected exactly one staging invocation');
      const row = rows[0];
      assert(
        row.status === 'settled' &&
          row.settlement_outcome === 'applied' &&
          row.operation_state === 'resume-admitted' &&
          typeof row.notification_id === 'string' &&
          row.notification_id === row.stored_notification_id &&
          typeof row.receipt_key === 'string' &&
          typeof row.receipt_thread_id === 'string' &&
          row.receipt_thread_id === row.thread_id &&
          row.receipt_notification_id === row.notification_id &&
          /^[a-f0-9]{64}$/.test(row.receipt_digest) &&
          row.source === 'flowsafe-staging' &&
          row.kind === 'host-owned-notification',
        'staging notification or receipt is unconfirmed',
      );
      const after = await counts();
      assert(
        baseline &&
          after.notifications === baseline.notifications + 1 &&
          after.receipts === baseline.receipts + 1,
        'staging notification or receipt count differs',
      );
    },
  };
}

export async function runNotificationStagingControl({
  probe,
  preflightD1,
  verifyD1,
  requestOffDeploy,
  onBeforeUnlock = async () => {},
}) {
  assert(
    typeof requestOffDeploy === 'function',
    'off deploy watchdog is required',
  );
  let initial;
  let unlockAttempted = false;
  let outcome;
  let failure;
  try {
    initial = await probe('GET', '/admin/execution-fence');
    exactFence(initial, initialFence);
    const maintenance = await probe('GET', '/admin/maintenance-status');
    assert(
      Number.isSafeInteger(maintenance.alarmAt) &&
        Number.isSafeInteger(maintenance.nextRemoteStepRecoveryAt) &&
        maintenance.alarmAt <=
          Math.max(Date.now(), maintenance.nextRemoteStepRecoveryAt) &&
        maintenance.lastRemoteStepRecoveryError === undefined,
      'notification recovery duty is not armed',
    );
    await preflightD1();
    await onBeforeUnlock();
    unlockAttempted = true;
    const opened = await probe('POST', '/admin/execution-fence', {
      expected: 'migration-locked',
      next: 'open',
      expectedMutationEpoch: initial.mutationEpoch,
      expectedRevision: initial.transitionRevision,
      advanceMutationEpoch: true,
    });
    exactFence(opened, nextFence(initial, 'open'));
    const started = await probe('POST', '/start');
    assert(
      started?.status === 'suspended' &&
        typeof started.invocationId === 'string' &&
        started.invocationId.length > 0,
      'staging start did not suspend',
    );
    const completed = await probe('POST', '/complete', {
      invocationId: started.invocationId,
    });
    assert(
      completed?.invocationId === started.invocationId &&
        completed.status === 'success',
      'staging completion did not settle',
    );
    await verifyD1(started.invocationId);
    outcome = {
      invocationId: started.invocationId,
      verdict: 'notification_staging_passed',
    };
  } catch (error) {
    failure = error;
  } finally {
    if (unlockAttempted) {
      try {
        const current = await probe('GET', '/admin/execution-fence');
        if (current.state === 'open') {
          assert(
            current.requireMutationEpoch === true,
            'open fence has wrong epoch policy',
          );
          assert(
            current.mutationEpoch === initial.mutationEpoch + 1,
            'open fence epoch differs',
          );
          assert(
            current.transitionRevision === initial.transitionRevision + 1,
            'open fence revision differs',
          );
          const locked = await probe('POST', '/admin/execution-fence', {
            expected: 'open',
            next: 'migration-locked',
            expectedMutationEpoch: current.mutationEpoch,
            expectedRevision: current.transitionRevision,
            advanceMutationEpoch: true,
          });
          exactFence(locked, nextFence(current, 'migration-locked'));
        } else {
          exactFence(current, initialFence);
          assert(!outcome, 'successful proof lost its open fence');
        }
      } catch (error) {
        failure = new AggregateError(
          failure ? [failure, error] : [error],
          'staging fence relock is unconfirmed',
        );
      }
    }
    try {
      await requestOffDeploy();
    } catch (error) {
      failure = new AggregateError(
        failure ? [failure, error] : [error],
        'staging off deploy is unconfirmed',
      );
    }
  }
  if (failure) throw failure;
  return outcome;
}
