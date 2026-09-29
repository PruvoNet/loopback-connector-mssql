// Copyright IBM Corp. 2015,2019. All Rights Reserved.
// Node module: loopback-connector-mssql
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
// Real-database check that a transaction's isolation level does not outlive it. SQL Server keeps
// SET TRANSACTION ISOLATION LEVEL on the session and mssql hands the connection back to the pool without
// resetting it, so every later plain read on that connection would run at the transaction's level.
// Same env as rollback-in-flight.js; skipped when MSSQL_HOST is unset.
const {describe, it, before, after} = require('node:test');
const assert = require('node:assert');
const {promisify} = require('node:util');
const mssql = require('mssql');
const {finishTransaction, discardOnServerAbort} = require('../../lib/transaction');

const host = process.env.MSSQL_HOST;
const config = host && {
  server: host,
  port: Number(process.env.MSSQL_PORT || 1433),
  user: process.env.MSSQL_USER || 'sa',
  password: process.env.MSSQL_PASSWORD,
  database: process.env.MSSQL_DATABASE || 'master',
  requestTimeout: 30000,
  // One connection, so the read after the transaction is guaranteed to reuse its session.
  pool: {max: 1, min: 0},
  options: {trustServerCertificate: true, enableArithAbort: true},
};

const READ_UNCOMMITTED = 1;
const READ_COMMITTED = 2;
const SERIALIZABLE = 4;

/**
 * Identity and isolation level of the session a query lands on. SQL Server reuses a SPID for the next login,
 * so the session is identified by SPID plus login time.
 * @param {mssql.ConnectionPool|mssql.Transaction} poolOrTx
 * @return {Promise<{session: string, level: number}>}
 */
async function sessionState(poolOrTx) {
  const result = await new mssql.Request(poolOrTx).query(
    'SELECT CONCAT(@@SPID, \'@\', CONVERT(varchar(30), login_time, 126)) AS session, ' +
    'transaction_isolation_level AS level FROM sys.dm_exec_sessions WHERE session_id = @@SPID',
  );
  return result.recordset[0];
}

/**
 * Begin a transaction at `level`, run one statement in it, and finish it through the connector.
 * @param {mssql.ConnectionPool} pool
 * @param {number} level mssql.ISOLATION_LEVEL value
 * @param {'commit'|'rollback'} operation
 * @return {Promise<{session: string, level: number}>} the session state inside the transaction
 */
async function runAndFinish(pool, level, operation) {
  const tx = new mssql.Transaction(pool);
  await tx.begin(level);
  const inside = await sessionState(tx);
  await promisify(finishTransaction)(tx, operation);
  return inside;
}

/**
 * Assert the next plain query reuses the transaction's session and runs at READ COMMITTED.
 * @param {mssql.ConnectionPool} pool
 * @param {{session: string}} inside
 */
async function assertSameSessionAtReadCommitted(pool, inside) {
  const after = await sessionState(pool);
  assert.strictEqual(after.session, inside.session, 'the read must reuse the transaction session');
  assert.strictEqual(after.level, READ_COMMITTED);
}

describe('isolation level does not leak to the pool', {skip: !config && 'MSSQL_HOST not set'}, () => {
  let pool;

  before(async () => {
    pool = await new mssql.ConnectionPool(config).connect();
  });

  after(async () => {
    if (pool) {
      await pool.close();
    }
  });

  for (const operation of ['commit', 'rollback']) {
    it(`restores READ COMMITTED after a READ UNCOMMITTED ${operation}`, async () => {
      const inside = await runAndFinish(pool, mssql.ISOLATION_LEVEL.READ_UNCOMMITTED, operation);

      assert.strictEqual(inside.level, READ_UNCOMMITTED);
      await assertSameSessionAtReadCommitted(pool, inside);
    });
  }

  it('restores READ COMMITTED after a SERIALIZABLE commit', async () => {
    const inside = await runAndFinish(pool, mssql.ISOLATION_LEVEL.SERIALIZABLE, 'commit');

    assert.strictEqual(inside.level, SERIALIZABLE);
    await assertSameSessionAtReadCommitted(pool, inside);
  });

  // The reset is itself a statement, so it is refused while another one is running; the rollback waits for
  // that statement and the retry must still restore the level.
  it('restores READ COMMITTED when the rollback had to wait for a running statement', async () => {
    const tx = new mssql.Transaction(pool);
    await tx.begin(mssql.ISOLATION_LEVEL.READ_UNCOMMITTED);
    const inside = await sessionState(tx);
    const started = Date.now();
    const running = (async () => {
      try {
        await new mssql.Request(tx).query('WAITFOR DELAY \'00:00:02\'');
        return 'ran';
      } catch (err) {
        return err.code;
      }
    })();

    await promisify(finishTransaction)(tx, 'rollback');

    assert.strictEqual(await running, 'ran', 'the statement must hold the connection, so the first reset is refused');
    assert.ok(Date.now() - started >= 1900, 'the rollback must have waited for the statement');
    await assertSameSessionAtReadCommitted(pool, inside);
  });

  it('leaves a READ COMMITTED transaction at READ COMMITTED', async () => {
    const inside = await runAndFinish(pool, mssql.ISOLATION_LEVEL.READ_COMMITTED, 'commit');

    assert.strictEqual(inside.level, READ_COMMITTED);
    await assertSameSessionAtReadCommitted(pool, inside);
  });

  // The server can end the transaction itself (XACT_ABORT, deadlock victim); mssql then releases the
  // connection before any reset can run, so that session must not be reused.
  it('does not reuse a session whose READ UNCOMMITTED transaction the server aborted', async () => {
    const tx = new mssql.Transaction(pool);
    await tx.begin(mssql.ISOLATION_LEVEL.READ_UNCOMMITTED);
    discardOnServerAbort(tx);
    const inside = await sessionState(tx);
    const err = await new mssql.Request(tx).batch('SET XACT_ABORT ON; SELECT 1/0').catch((e) => e);
    await promisify(finishTransaction)(tx, 'rollback');

    assert.strictEqual(err.number, 8134);
    const after = await sessionState(pool);
    assert.notStrictEqual(after.session, inside.session, 'the aborted session must be replaced');
    assert.strictEqual(after.level, READ_COMMITTED);
  });
});
