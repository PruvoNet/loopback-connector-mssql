// Copyright IBM Corp. 2015,2019. All Rights Reserved.
// Node module: loopback-connector-mssql
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
// Runs with `node --test test/unit` and needs no database.
const {describe, it, beforeEach, afterEach} = require('node:test');
const assert = require('node:assert');
const {MsSQL} = require('../../lib/mssql');

// strong-globalize's g.warn writes through console; capture both channels it may use.
function captureConsole() {
  const lines = [];
  const original = {warn: console.warn, error: console.error};
  console.warn = (...args) => lines.push(args.join(' '));
  console.error = (...args) => lines.push(args.join(' '));
  return {
    lines,
    restore() {
      console.warn = original.warn;
      console.error = original.error;
    },
  };
}

describe('executeSQL with a transaction that has already ended (PRV-7808)', function() {
  let captured;
  beforeEach(function() {
    captured = captureConsole();
  });
  afterEach(function() {
    captured.restore();
  });

  it('still runs the statement on the pool, as before, and warns with instructions', function() {
    const connector = {client: {}};
    const transaction = {connector, connection: null, id: 'tx-1'};
    // Reaching `new mssql.Request(pool)` with a fake pool throws, which proves the statement was not refused.
    assert.throws(function() {
      MsSQL.prototype.executeSQL.call(connector, 'UPDATE T SET n = 1', [], {transaction}, function() {});
    });
    assert.strictEqual(captured.lines.length, 1);
    const line = captured.lines[0];
    assert.match(line, /ended transaction \(tx-1\)/);
    assert.match(line, /UPDATE T SET n = 1/);
    assert.match(line, /delete `options.transaction`/);
  });

  it('does not warn for an active transaction of this connector', function() {
    const connector = {client: {}};
    const transaction = {connector, connection: {}, id: 'tx-2'};
    assert.throws(function() {
      MsSQL.prototype.executeSQL.call(connector, 'SELECT 1', [], {transaction}, function() {});
    });
    assert.deepStrictEqual(captured.lines, []);
  });

  it('does not warn for a transaction of another connector', function() {
    const connector = {client: {}};
    const foreign = {connector: {}, connection: null, id: 'tx-other'};
    assert.throws(function() {
      MsSQL.prototype.executeSQL.call(connector, 'SELECT 1', [], {transaction: foreign}, function() {});
    });
    assert.deepStrictEqual(captured.lines, []);
  });
});
