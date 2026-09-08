// Copyright IBM Corp. 2015,2019. All Rights Reserved.
// Node module: loopback-connector-mssql
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
const debug = require('debug')('loopback:connector:mssql:transaction');

// How often to re-check whether the in-flight request has released the connection (PRV-7808).
const REQUEST_IN_PROGRESS_POLL_MS = 100;

// Upper bound for that wait when the connection config carries no `requestTimeout`.
const DEFAULT_MAX_WAIT_MS = 5 * 60 * 1000;

// After cancelling a request that outlived the deadline, how long to wait for it to end before closing the
// connection under it, and again before giving up. Capped so short request timeouts get short graces.
const CANCEL_GRACE_CAP_MS = 30 * 1000;

// Per-transaction queue of pending commit/rollback calls, so two callers can never reach the driver
// for the same transaction at the same time (the second one would find the connection already gone
// and throw inside the driver's callback).
const FINISH_QUEUE = '_loopbackFinishQueue';

module.exports = mixinTransaction;
module.exports.finishTransaction = finishTransaction;
module.exports.releaseFailedBegin = releaseFailedBegin;

/**
 * Commit or roll back a driver transaction, waiting for any in-flight request first.
 *
 * mssql refuses to commit or roll back while one of the transaction's requests is still running
 * (`EREQINPROG`). The caller above us (loopback-datasource-juggler) drops its handle to the
 * transaction whether we succeed or fail, so a refusal here left the pool connection borrowed forever
 * (PRV-7808). We wait for the request to end and then finish the transaction for real.
 *
 * The wait ends when one of these happens:
 * - the driver clears its in-progress marker (normal end, error, or the driver's own request timeout);
 * - the server aborted the transaction meanwhile (the driver then drops the connection field and
 *   releases the connection itself, but never clears the in-progress marker);
 * - the request outlives the configured `requestTimeout`: we cancel it, which makes it end.
 *
 * A rollback that had to wait is then performed. A commit that had to wait is refused (`EREQINPROG`)
 * and the transaction is rolled back instead: the caller asked to commit before a statement had
 * finished, so nobody knows how that statement ended, and committing could persist a partial
 * transaction. A caller that awaits its statements before committing never hits this.
 *
 * Calls for the same transaction run one after another. A rollback that arrives after the transaction
 * already ended finds nothing to do and reports success (`ENOTBEGUN`, or `EABORT` when the server
 * aborted it and the driver released the connection): the connection is already back in the pool and
 * the caller only wants the transaction ended. A commit in that state fails with the driver's error,
 * because nothing was committed.
 *
 * @param {Transaction} transaction mssql transaction
 * @param {'commit'|'rollback'} operation
 * @param {Function} cb
 */
function finishTransaction(transaction, operation, cb) {
  const previous = transaction[FINISH_QUEUE] || Promise.resolve();
  const current = previous.then(function() {
    return new Promise(function(resolve) {
      attempt(transaction, operation, resolve);
    });
  });
  transaction[FINISH_QUEUE] = current;
  current.then(function(err) {
    if (transaction[FINISH_QUEUE] === current) {
      delete transaction[FINISH_QUEUE];
    }
    // Outside the promise chain: a throwing callback surfaces as it would from any callback API,
    // instead of turning into an unhandled rejection that nobody sees.
    setImmediate(cb, err || undefined);
  });
}

/**
 * One try of `operation` on the driver transaction, with the wait-and-retry logic around a refusal.
 *
 * `waitedOn` is the request the previous wait was for. A refusal caused by a *new* request (the caller
 * issued another statement meanwhile) is waited for again; a refusal by the *same* request means the
 * wait gave up on it, and the refusal goes to the caller instead of starting another wait. So every
 * call is terminal, while a flow that keeps issuing statements is still cleaned up behind.
 *
 * @param {Transaction} transaction mssql transaction
 * @param {'commit'|'rollback'} operation
 * @param {Function} done Called with the driver error, or null
 * @param {Object} [waitedOn] The request the previous wait was for
 */
function attempt(transaction, operation, done, waitedOn) {
  transaction[operation](function(err) {
    if (!err) {
      return done(null);
    }
    const request = transaction._activeRequest;
    if (err.code === 'EREQINPROG' && request && request !== waitedOn) {
      debug('%s refused: request in progress, waiting for it to release the connection', operation);
      return waitForRequestToEnd(transaction, request, Date.now() + maxWaitMs(transaction), function() {
        if (operation === 'commit') {
          // The caller asked to commit before one of the transaction's statements had finished, so nobody
          // knows how that statement ended (it may have timed out or been cancelled). Committing now could
          // persist a partial transaction. Roll back instead and tell the caller the commit did not happen.
          debug('commit refused: a statement was still running when commit was requested, rolling back instead');
          return attempt(transaction, 'rollback', function(rollbackErr) {
            done(refusedCommitError(rollbackErr));
          }, request);
        }
        attempt(transaction, operation, done, request);
      });
    }
    if (err.code === 'EREQINPROG') {
      debug('%s still refused after the wait gave up on the same request, reporting it to the caller', operation);
      return done(err);
    }
    if (operation === 'rollback' && (err.code === 'EABORT' || err.code === 'ENOTBEGUN')) {
      debug('rollback found no open transaction (%s), treating as rolled back', err.code);
      return done(null);
    }
    done(err);
  });
}

/**
 * How long to wait for an in-flight request before cancelling it: the connection's `requestTimeout`,
 * or a default when the config carries none.
 *
 * @param {Transaction} transaction mssql transaction
 * @return {number} milliseconds
 */
function maxWaitMs(transaction) {
  const config = transaction.config || (transaction.parent && transaction.parent.config);
  const requestTimeout = config && Number(config.requestTimeout);
  return requestTimeout > 0 ? requestTimeout : DEFAULT_MAX_WAIT_MS;
}

/**
 * The error a caller gets when its commit arrived while a statement was still running and the
 * transaction was rolled back instead.
 *
 * @param {Error} [rollbackErr] Error from that rollback, if it failed
 * @return {Error} with `code` EREQINPROG and, when present, `rollbackError`
 */
function refusedCommitError(rollbackErr) {
  const err = new Error('Commit refused: a statement of this transaction was still running when commit was ' +
    'requested, so its outcome is unknown; the transaction was rolled back instead');
  err.code = 'EREQINPROG';
  if (rollbackErr) {
    err.rollbackError = rollbackErr;
  }
  return err;
}

/**
 * Waits for one specific in-flight `request` and calls `cb(cancelled)` once it is no longer the active
 * request; `cancelled` is true when this helper had to cancel it because it outlived the deadline. If the
 * caller started another statement meanwhile, the wait returns at once and `attempt` starts a fresh
 * deadline for that new request, so a deadline never carries over from one request to the next.
 *
 * The wait is terminal in every case:
 * 1. at `deadline` the request is cancelled;
 * 2. one grace later, if it is still running, the connection under it is closed, which ends the request
 *    at the socket level and makes the driver release the connection;
 * 3. one more grace later the wait gives up and the caller gets the driver's refusal instead of hanging.
 *
 * @param {Transaction} transaction mssql transaction
 * @param {Object} request The driver request to wait for
 * @param {number} deadline Epoch milliseconds at which the request is cancelled
 * @param {Function} cb Called with `cancelled`
 * @param {boolean} [cancelled] Internal, carried across polls
 */
function waitForRequestToEnd(transaction, request, deadline, cb, cancelled) {
  if (transaction._activeRequest !== request || !transaction._acquiredConnection || transaction._aborted) {
    return setImmediate(cb, Boolean(cancelled));
  }
  const now = Date.now();
  const grace = Math.min(CANCEL_GRACE_CAP_MS, maxWaitMs(transaction));
  if (now >= deadline && !request._loopbackCancelled) {
    request._loopbackCancelled = true;
    cancelled = true;
    if (typeof request.cancel === 'function') {
      debug('request still running after %d ms, cancelling it', maxWaitMs(transaction));
      request.cancel();
    } else {
      debug('request still running after %d ms and cannot be cancelled', maxWaitMs(transaction));
    }
  }
  if (now >= deadline + grace && !request._loopbackConnectionClosed) {
    request._loopbackConnectionClosed = true;
    const connection = transaction._acquiredConnection;
    if (connection && typeof connection.close === 'function') {
      debug('request still running %d ms after cancel, closing its connection', grace);
      connection.close();
    }
  }
  if (now >= deadline + 2 * grace) {
    debug('request still running after cancel and close, giving up the wait');
    return setImmediate(cb, Boolean(cancelled));
  }
  setTimeout(function() {
    waitForRequestToEnd(transaction, request, deadline, cb, cancelled);
  }, REQUEST_IN_PROGRESS_POLL_MS);
}

/**
 * Give the pool its connection back when `BEGIN TRANSACTION` failed after the connection was acquired.
 * The driver keeps the connection on the transaction in that case and the application never gets a
 * handle to roll back, so without this the connection stays borrowed.
 *
 * @param {Transaction} transaction mssql transaction whose begin() failed
 */
function releaseFailedBegin(transaction) {
  const connection = transaction._acquiredConnection;
  if (!connection) {
    return;
  }
  debug('begin failed after the connection was acquired, releasing it');
  if (typeof transaction._abort === 'function' && typeof connection.removeListener === 'function') {
    connection.removeListener('rollbackTransaction', transaction._abort);
  }
  transaction._acquiredConnection = null;
  transaction._acquiredConfig = null;
  transaction.parent.release(connection);
}

/*!
 * @param {MsSQL} MsSQL connector class
 */
function mixinTransaction(MsSQL, mssql) {
  /**
   * Begin a new transaction
   * @param isolationLevel
   * @param cb
   */
  MsSQL.prototype.beginTransaction = function(isolationLevel, cb) {
    debug('Begin a transaction with isolation level: %s', isolationLevel);
    isolationLevel = mssql.ISOLATION_LEVEL[isolationLevel.replace(' ', '_')];
    const transaction = new mssql.Transaction(this.client);
    transaction.begin(isolationLevel, function(err) {
      if (err) {
        releaseFailedBegin(transaction);
      }
      cb(err, transaction);
    });
  };

  /**
   *
   * @param connection
   * @param cb
   */
  MsSQL.prototype.commit = function(connection, cb) {
    debug('Commit a transaction');
    finishTransaction(connection, 'commit', cb);
  };

  /**
   *
   * @param connection
   * @param cb
   */
  MsSQL.prototype.rollback = function(connection, cb) {
    debug('Rollback a transaction');
    finishTransaction(connection, 'rollback', cb);
  };
}
