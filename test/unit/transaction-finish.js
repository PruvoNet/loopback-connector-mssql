// Copyright IBM Corp. 2015,2019. All Rights Reserved.
// Node module: loopback-connector-mssql
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
// Runs with `node --test test/unit` and needs no database: the driver transaction is a stub that
// behaves like mssql's Transaction (refuses with EREQINPROG while `_activeRequest` is set).
const {describe, it} = require('node:test');
const assert = require('node:assert');
const mixinTransaction = require('../../lib/transaction');
const {finishTransaction} = mixinTransaction;

/**
 * An error shaped like the driver's, with a `code`.
 * @param {string} code
 * @return {Error}
 */
function driverError(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

/**
 * Stub of mssql.Transaction: `failWith` lists the error codes to return on successive calls
 * (undefined = success). `_activeRequest` mirrors the driver's in-progress marker.
 * @param {string[]} failWith
 * @return {Object} the stub, with `calls` counters
 */
function stubTransaction(failWith) {
  const calls = {commit: 0, rollback: 0};
  const tx = {
    _activeRequest: null,
    _acquiredConnection: {},
    calls,
  };
  ['commit', 'rollback'].forEach(function(op) {
    tx[op] = function(cb) {
      calls[op]++;
      const code = failWith.shift();
      setImmediate(cb, code ? driverError(code) : null);
    };
  });
  return tx;
}

describe('finishTransaction', function() {
  it('rolls back once when the driver accepts', function(_t, done) {
    const tx = stubTransaction([]);
    finishTransaction(tx, 'rollback', function(err) {
      assert.ifError(err);
      assert.strictEqual(tx.calls.rollback, 1);
      done();
    });
  });

  it('waits for the in-flight request to end, then rolls back (PRV-7808)', function(_t, done) {
    const tx = stubTransaction(['EREQINPROG']);
    tx._activeRequest = {};
    // The request finishes later; the driver clears `_activeRequest` before calling back.
    setTimeout(function() {
      tx._activeRequest = null;
    }, 250);
    finishTransaction(tx, 'rollback', function(err) {
      assert.ifError(err);
      assert.strictEqual(tx.calls.rollback, 2);
      done();
    });
  });

  it('keeps waiting while the caller keeps starting new statements', function(_t, done) {
    const tx = stubTransaction(['EREQINPROG', 'EREQINPROG']);
    tx._activeRequest = {};
    // The first statement ends and the caller immediately starts another one; the driver refuses again.
    setTimeout(function() {
      tx._activeRequest = {};
    }, 150);
    setTimeout(function() {
      tx._activeRequest = null;
    }, 300);
    finishTransaction(tx, 'rollback', function(err) {
      assert.ifError(err);
      assert.strictEqual(tx.calls.rollback, 3);
      done();
    });
  });

  it('treats a rollback of an already aborted transaction as done', function(_t, done) {
    const tx = stubTransaction(['EABORT']);
    finishTransaction(tx, 'rollback', function(err) {
      assert.ifError(err);
      done();
    });
  });

  it('treats a rollback of a transaction that never began as done', function(_t, done) {
    const tx = stubTransaction(['ENOTBEGUN']);
    finishTransaction(tx, 'rollback', function(err) {
      assert.ifError(err);
      done();
    });
  });

  it('reports any other rollback error', function(_t, done) {
    const tx = stubTransaction(['ESOCKET']);
    finishTransaction(tx, 'rollback', function(err) {
      assert.strictEqual(err && err.code, 'ESOCKET');
      done();
    });
  });

  it('refuses a commit requested while a request is in flight, and rolls back once it ended', function(_t, done) {
    const tx = stubTransaction(['EREQINPROG']);
    tx._activeRequest = {};
    setTimeout(function() {
      tx._activeRequest = null;
    }, 150);
    finishTransaction(tx, 'commit', function(err) {
      assert.strictEqual(err && err.code, 'EREQINPROG');
      assert.match(err.message, /rolled back instead/);
      assert.strictEqual(tx.calls.commit, 1);
      assert.strictEqual(tx.calls.rollback, 1);
      done();
    });
  });

  it('does not hide an aborted transaction from commit', function(_t, done) {
    const tx = stubTransaction(['EABORT']);
    finishTransaction(tx, 'commit', function(err) {
      assert.strictEqual(err && err.code, 'EABORT');
      done();
    });
  });
});

/**
 * Stub closer to mssql's tedious Transaction: the pre-checks read `_acquiredConnection` and
 * `_activeRequest`, and the connection is dropped only when the driver call completes. Two calls
 * reaching the driver in the same tick both pass the pre-checks; the second completion then crashes.
 * @return {Object} the stub, with `sent` listing the operations that reached the driver
 */
function driverLikeTransaction() {
  const tx = {_activeRequest: {}, _acquiredConnection: {}, sent: []};
  ['commit', 'rollback'].forEach(function(op) {
    tx[op] = function(cb) {
      if (!this._acquiredConnection) {
        return setImmediate(cb, driverError('ENOTBEGUN'));
      }
      if (this._activeRequest) {
        return setImmediate(cb, driverError('EREQINPROG'));
      }
      tx.sent.push(op);
      setImmediate(function() {
        if (!tx._acquiredConnection) {
          throw new TypeError('Cannot read properties of null');
        }
        tx._acquiredConnection = null;
        cb(null);
      });
    };
  });
  return tx;
}

describe('finishTransaction with overlapping calls on one transaction', function() {
  it('runs a commit requested during a pending rollback after it, and the commit fails', function(_t, done) {
    const tx = driverLikeTransaction();
    const results = {};
    finishTransaction(tx, 'rollback', function(err) {
      results.rollback = err || null;
    });
    finishTransaction(tx, 'commit', function(err) {
      results.commit = err;
      assert.deepStrictEqual(tx.sent, ['rollback']);
      assert.strictEqual(results.rollback, null);
      assert.strictEqual(err && err.code, 'ENOTBEGUN');
      done();
    });
    setTimeout(function() {
      tx._activeRequest = null;
    }, 150);
  });

  it('runs two overlapping rollbacks one after another, both report success', function(_t, done) {
    const tx = driverLikeTransaction();
    let first;
    finishTransaction(tx, 'rollback', function(err) {
      first = err || null;
    });
    finishTransaction(tx, 'rollback', function(err) {
      assert.ifError(err);
      assert.strictEqual(first, null);
      assert.deepStrictEqual(tx.sent, ['rollback']);
      done();
    });
    setTimeout(function() {
      tx._activeRequest = null;
    }, 150);
  });

  it('a rollback requested during a pending commit runs after it and reports success', function(_t, done) {
    const tx = driverLikeTransaction();
    let commitErr;
    finishTransaction(tx, 'commit', function(err) {
      commitErr = err;
    });
    finishTransaction(tx, 'rollback', function(err) {
      assert.ifError(err);
      // The commit had to wait for a running statement, so it was turned into a rollback and refused.
      assert.strictEqual(commitErr && commitErr.code, 'EREQINPROG');
      assert.deepStrictEqual(tx.sent, ['rollback']);
      done();
    });
    setTimeout(function() {
      tx._activeRequest = null;
    }, 150);
  });

  it('stops waiting when the server aborts the transaction during the wait (rollback succeeds)', function(_t, done) {
    const tx = driverLikeTransaction();
    finishTransaction(tx, 'rollback', function(err) {
      assert.ifError(err);
      assert.deepStrictEqual(tx.sent, []);
      done();
    });
    // The driver's abort handler drops the connection and releases it, but never clears _activeRequest.
    setTimeout(function() {
      tx._acquiredConnection = null;
      tx._aborted = true;
      tx.rollback = function(cb) {
        setImmediate(cb, driverError('EABORT'));
      };
    }, 150);
  });

  it('stops waiting when the server aborts the transaction during the wait (commit fails)', function(_t, done) {
    const tx = driverLikeTransaction();
    finishTransaction(tx, 'commit', function(err) {
      // Refused because a statement was running when commit was asked; the rollback finds the
      // transaction already aborted and counts as done, so no rollback error is attached.
      assert.strictEqual(err && err.code, 'EREQINPROG');
      assert.strictEqual(err.rollbackError, undefined);
      assert.deepStrictEqual(tx.sent, []);
      done();
    });
    setTimeout(function() {
      tx._acquiredConnection = null;
      tx._aborted = true;
      tx.commit = function(cb) {
        setImmediate(cb, driverError('EABORT'));
      };
    }, 150);
  });

  it('cancels a request that outlives requestTimeout, then rolls back', function(_t, done) {
    const tx = driverLikeTransaction();
    tx.config = {requestTimeout: 200};
    const request = {
      cancel: function() {
        request.cancelled = true;
        // The driver ends a cancelled request and clears the marker shortly after.
        setTimeout(function() {
          tx._activeRequest = null;
        }, 50);
      },
    };
    tx._activeRequest = request;
    const started = Date.now();
    finishTransaction(tx, 'rollback', function(err) {
      assert.ifError(err);
      assert.strictEqual(request.cancelled, true);
      assert.deepStrictEqual(tx.sent, ['rollback']);
      assert.ok(Date.now() - started >= 200);
      done();
    });
  });

  it('refuses a waiting commit after cancelling a statement, and rolls back instead', function(_t, done) {
    const tx = driverLikeTransaction();
    tx.config = {requestTimeout: 200};
    const request = {
      cancel: function() {
        request.cancelled = true;
        setTimeout(function() {
          tx._activeRequest = null;
        }, 50);
      },
    };
    tx._activeRequest = request;
    finishTransaction(tx, 'commit', function(err) {
      assert.strictEqual(err && err.code, 'EREQINPROG');
      assert.strictEqual(request.cancelled, true);
      assert.deepStrictEqual(tx.sent, ['rollback']);
      done();
    });
  });

  it('closes the connection when a cancelled request does not end, then rolls back', function(_t, done) {
    const tx = driverLikeTransaction();
    tx.config = {requestTimeout: 200};
    const request = {cancel: function() { request.cancelled = true; }}; // cancel has no effect
    tx._acquiredConnection.close = function() {
      tx._acquiredConnection.closed = true;
      // The driver ends the request once its socket is gone.
      setTimeout(function() {
        tx._activeRequest = null;
      }, 50);
    };
    tx._activeRequest = request;
    const started = Date.now();
    finishTransaction(tx, 'rollback', function(err) {
      assert.ifError(err);
      assert.strictEqual(request.cancelled, true);
      assert.deepStrictEqual(tx.sent, ['rollback']);
      assert.ok(Date.now() - started >= 400, 'cancel at 200 ms, close one grace later');
      done();
    });
  });

  it('gives up after cancel and close both fail, and reports the refusal instead of hanging', {timeout: 5000},
    function(_t, done) {
      const tx = driverLikeTransaction();
      tx.config = {requestTimeout: 200};
      tx._activeRequest = {}; // no cancel(), and nothing ever clears it
      tx._acquiredConnection.close = function() {}; // no effect either
      const started = Date.now();
      finishTransaction(tx, 'rollback', function(err) {
        assert.strictEqual(err && err.code, 'EREQINPROG');
        assert.deepStrictEqual(tx.sent, []);
        assert.ok(Date.now() - started >= 600, 'deadline plus two graces');
        done();
      });
    });

  it('gives a new request its own deadline instead of the previous request\'s', function(_t, done) {
    const tx = driverLikeTransaction();
    tx.config = {requestTimeout: 200};
    const first = {cancel: function() { first.cancelled = true; }};
    const second = {cancel: function() { second.cancelled = true; }};
    tx._activeRequest = first;
    // The first statement ends at 100 ms and the caller immediately starts a second one, which runs until
    // 350 ms: past the first request's deadline (200 ms) but within its own (100 + 200 = 300 ms would be
    // the earliest cancel; the driver ends it at 350 ms). Under the old code it was cancelled at 200 ms.
    setTimeout(function() {
      tx._activeRequest = second;
    }, 100);
    setTimeout(function() {
      tx._activeRequest = null;
    }, 350);
    finishTransaction(tx, 'rollback', function(err) {
      assert.ifError(err);
      assert.strictEqual(first.cancelled, undefined);
      assert.strictEqual(second.cancelled, true, 'second request is cancelled at its own deadline, ~300 ms');
      assert.deepStrictEqual(tx.sent, ['rollback']);
      done();
    });
  });

  it('commits normally when no request is in flight', function(_t, done) {
    const tx = driverLikeTransaction();
    tx._activeRequest = null;
    finishTransaction(tx, 'commit', function(err) {
      assert.ifError(err);
      assert.deepStrictEqual(tx.sent, ['commit']);
      done();
    });
  });

  it('clears its queue so a later call on the same object starts fresh', function(_t, done) {
    const tx = driverLikeTransaction();
    tx._activeRequest = null;
    finishTransaction(tx, 'commit', function(err) {
      assert.ifError(err);
      assert.strictEqual(tx._loopbackFinishQueue, undefined);
      done();
    });
  });
});

describe('beginTransaction when BEGIN fails after the connection was acquired', function() {
  it('releases the connection back to the pool and reports the error', function(_t, done) {
    const released = [];
    const connection = {removeListener: function() {}};
    const fakePool = {release: function(c) { released.push(c); }};
    // Fake mssql: Transaction whose begin() acquires the connection and then fails, like a dropped socket.
    const fakeMssql = {
      ISOLATION_LEVEL: {READ_UNCOMMITTED: 1},
      Transaction: function(parent) {
        this.parent = parent;
        this._abort = function() {};
        this.begin = function(_level, cb) {
          this._acquiredConnection = connection;
          this._acquiredConfig = {};
          setImmediate(cb, driverError('ESOCKET'));
        };
      },
    };
    function FakeConnector() {
      this.client = fakePool;
    }
    mixinTransaction(FakeConnector, fakeMssql);

    new FakeConnector().beginTransaction('READ UNCOMMITTED', function(err, tx) {
      assert.strictEqual(err && err.code, 'ESOCKET');
      assert.deepStrictEqual(released, [connection]);
      assert.strictEqual(tx._acquiredConnection, null);
      done();
    });
  });

  it('leaves a successful begin untouched', function(_t, done) {
    const released = [];
    const fakeMssql = {
      ISOLATION_LEVEL: {READ_UNCOMMITTED: 1},
      Transaction: function(parent) {
        this.parent = parent;
        this.begin = function(_level, cb) {
          this._acquiredConnection = {};
          setImmediate(cb, null);
        };
      },
    };
    function FakeConnector() {
      this.client = {release: function(c) { released.push(c); }};
    }
    mixinTransaction(FakeConnector, fakeMssql);

    new FakeConnector().beginTransaction('READ UNCOMMITTED', function(err, tx) {
      assert.ifError(err);
      assert.ok(tx._acquiredConnection);
      assert.deepStrictEqual(released, []);
      done();
    });
  });
});
