// Copyright IBM Corp. 2015,2019. All Rights Reserved.
// Node module: loopback-connector-mssql
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
// Real-database check for PRV-7808. Runs with `node --test test/integration` against the SQL Server named by
// MSSQL_HOST / MSSQL_PORT / MSSQL_USER / MSSQL_PASSWORD (skipped when MSSQL_HOST is unset).
//
// It reproduces the production failure: a rollback requested while a statement of the transaction is still
// running. With the plain driver call the rollback is refused and the pool connection stays borrowed. With
// finishTransaction the rollback completes once the statement ends, the connection returns to the pool and
// the row locks the transaction held are released.
const {describe, it, before, after} = require('node:test');
const assert = require('node:assert');
const mssql = require('mssql');
const {finishTransaction} = require('../../lib/transaction');

const host = process.env.MSSQL_HOST;
const config = host && {
  server: host,
  port: Number(process.env.MSSQL_PORT || 1433),
  user: process.env.MSSQL_USER || 'sa',
  password: process.env.MSSQL_PASSWORD,
  database: process.env.MSSQL_DATABASE || 'master',
  requestTimeout: 30000,
  pool: {max: 3, min: 0},
  options: {trustServerCertificate: true, enableArithAbort: true},
};

const LOCK_TABLE = 'tempdb..prv7808_lock';
const SLOW_STATEMENT = 'WAITFOR DELAY \'00:00:03\'';

function query(pool, sql) {
  return new mssql.Request(pool).query(sql);
}

// Start `sql` inside `tx` and return a promise of its outcome plus a settled flag, without awaiting it.
function startInTransaction(tx, sql) {
  const state = {settled: false};
  state.promise = new mssql.Request(tx).query(sql).then(
    () => {
      state.settled = true;
    },
    (err) => {
      state.settled = true;
      state.error = err;
    },
  );
  return state;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function rollbackViaConnector(tx) {
  return new Promise((resolve, reject) => {
    finishTransaction(tx, 'rollback', (err) => (err ? reject(err) : resolve()));
  });
}

describe('rollback while a statement is in flight (PRV-7808)', {skip: !config && 'MSSQL_HOST not set'}, () => {
  let pool;

  before(async () => {
    pool = await new mssql.ConnectionPool(config).connect();
    await query(pool, `IF OBJECT_ID('${LOCK_TABLE}') IS NOT NULL DROP TABLE ${LOCK_TABLE}`);
    await query(pool, `CREATE TABLE ${LOCK_TABLE} (id INT PRIMARY KEY, n INT NOT NULL)`);
    await query(pool, `INSERT INTO ${LOCK_TABLE} (id, n) VALUES (1, 0)`);
  });

  after(async () => {
    if (pool) {
      await query(pool, `IF OBJECT_ID('${LOCK_TABLE}') IS NOT NULL DROP TABLE ${LOCK_TABLE}`).catch(() => undefined);
      await pool.close();
    }
  });

  it('plain driver rollback is refused and leaves the connection borrowed (the defect)', async () => {
    const tx = new mssql.Transaction(pool);
    await tx.begin();
    const slow = startInTransaction(tx, SLOW_STATEMENT);
    await delay(300);

    await assert.rejects(tx.rollback(), (err) => err.code === 'EREQINPROG');
    await slow.promise;
    assert.ifError(slow.error);
    // The statement is over, nobody rolled back: the connection is still out of the pool.
    assert.strictEqual(pool.borrowed, 1);

    await tx.rollback();
    assert.strictEqual(pool.borrowed, 0);
  });

  it('finishTransaction waits for the statement, rolls back and returns the connection', async () => {
    const tx = new mssql.Transaction(pool);
    await tx.begin();
    const slow = startInTransaction(tx, SLOW_STATEMENT);
    await delay(300);

    const rollback = rollbackViaConnector(tx);
    // Rollback must not complete before the statement does.
    await delay(500);
    assert.strictEqual(slow.settled, false);

    await rollback;
    assert.strictEqual(slow.settled, true);
    assert.ifError(slow.error);
    assert.strictEqual(pool.borrowed, 0);
  });

  it('works when the in-flight statement is a later one in a multi-statement transaction', async () => {
    const tx = new mssql.Transaction(pool);
    await tx.begin();
    await new mssql.Request(tx).query(`UPDATE ${LOCK_TABLE} SET n = n + 1 WHERE id = 1`);
    const slow = startInTransaction(tx, SLOW_STATEMENT);
    await delay(300);

    await rollbackViaConnector(tx);
    assert.strictEqual(slow.settled, true);
    assert.strictEqual(pool.borrowed, 0);

    // The row lock taken by the UPDATE is gone and the update itself was undone.
    const result = await query(pool, `SET LOCK_TIMEOUT 2000; SELECT n FROM ${LOCK_TABLE} WITH (UPDLOCK) WHERE id = 1`);
    assert.strictEqual(result.recordset[0].n, 0);
  });

  it('does not wait forever when the server aborts the transaction while the rollback is waiting', async () => {
    const tx = new mssql.Transaction(pool);
    await tx.begin();
    // XACT_ABORT ON makes the conversion error abort the whole transaction on the server after 1 s;
    // the driver then drops its connection field without clearing the in-progress marker.
    const slow = startInTransaction(tx,
      'SET XACT_ABORT ON; WAITFOR DELAY \'00:00:01\'; SELECT CAST(\'not a number\' AS INT)');
    await delay(300);

    const started = Date.now();
    await rollbackViaConnector(tx);
    assert.ok(Date.now() - started < 5000, 'rollback must return soon after the abort');
    await slow.promise;
    assert.ok(slow.error, 'the statement itself fails');
    assert.strictEqual(pool.borrowed, 0);
  });

  it('cancels a statement that outlives requestTimeout and still returns the connection', async () => {
    const shortPool = await new mssql.ConnectionPool({...config, requestTimeout: 500}).connect();
    try {
      const tx = new mssql.Transaction(shortPool);
      await tx.begin();
      // A first result packet, then a long wait: exercises the case where the driver's own timer may
      // no longer protect us and only our cancel does.
      const slow = startInTransaction(tx, 'SELECT 1; WAITFOR DELAY \'00:00:10\'');
      await delay(100);

      const started = Date.now();
      await rollbackViaConnector(tx);
      assert.ok(Date.now() - started < 4000, 'rollback must not wait for the full 10 s statement');
      await slow.promise;
      assert.ok(slow.error, 'the statement ends with a timeout or cancel error');
      assert.strictEqual(shortPool.borrowed, 0);
    } finally {
      await shortPool.close();
    }
  });

  it('refuses a commit requested while a statement is still running, rolls back, persists nothing', async () => {
    const shortPool = await new mssql.ConnectionPool({...config, requestTimeout: 500}).connect();
    try {
      const before = (await query(shortPool, `SELECT n FROM ${LOCK_TABLE} WHERE id = 1`)).recordset[0].n;
      const tx = new mssql.Transaction(shortPool);
      await tx.begin();
      await new mssql.Request(tx).query(`UPDATE ${LOCK_TABLE} SET n = n + 1 WHERE id = 1`);
      // Ends by the driver's timeout or by our cancel; either way the commit must not go through.
      const slow = startInTransaction(tx, 'SELECT 1; WAITFOR DELAY \'00:00:10\'');
      await delay(100);

      await assert.rejects(
        new Promise((resolve, reject) => finishTransaction(tx, 'commit', (err) => (err ? reject(err) : resolve()))),
        (err) => err.code === 'EREQINPROG' && /rolled back instead/.test(err.message),
      );
      await slow.promise;
      assert.strictEqual(shortPool.borrowed, 0);
      const after = (await query(shortPool, `SELECT n FROM ${LOCK_TABLE} WHERE id = 1`)).recordset[0].n;
      assert.strictEqual(after, before, 'the first update must not be committed on its own');
    } finally {
      await shortPool.close();
    }
  });

  it('commits normally when every statement has finished', async () => {
    const before = (await query(pool, `SELECT n FROM ${LOCK_TABLE} WHERE id = 1`)).recordset[0].n;
    const tx = new mssql.Transaction(pool);
    await tx.begin();
    await new mssql.Request(tx).query(`UPDATE ${LOCK_TABLE} SET n = n + 1 WHERE id = 1`);
    await new Promise((resolve, reject) => finishTransaction(tx, 'commit', (err) => (err ? reject(err) : resolve())));
    assert.strictEqual(pool.borrowed, 0);
    const after = (await query(pool, `SELECT n FROM ${LOCK_TABLE} WHERE id = 1`)).recordset[0].n;
    assert.strictEqual(after, before + 1);
  });

  it('a rollback after the statement failed on its own is accepted first time', async () => {
    const tx = new mssql.Transaction(pool);
    await tx.begin();
    const failed = startInTransaction(tx, 'SELECT 1/0');
    await failed.promise;
    assert.ok(failed.error, 'statement should fail');

    await rollbackViaConnector(tx);
    assert.strictEqual(pool.borrowed, 0);
  });
});
