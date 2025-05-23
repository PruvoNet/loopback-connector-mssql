// Copyright IBM Corp. 2013,2019. All Rights Reserved.
// Node module: loopback-connector-mssql
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
const g = require('strong-globalize')();

/*! Module dependencies */
const mssql = require('mssql');
const SqlConnector = require('loopback-connector').SqlConnector;
const ParameterizedSQL = SqlConnector.ParameterizedSQL;
const util = require('util');
const debug = require('debug')('loopback:connector:mssql');
let LoopBackContext;
try {
  LoopBackContext = require('loopback-context');
} catch (err) {
}

mssql.map.register(Number, mssql.BigInt);

const name = 'mssql';

exports.name = name;
exports.initialize = function initializeSchema(dataSource, callback) {
  const settings = dataSource.settings || {};
  debug('Settings: %j', settings);
  const driver = new MsSQL(settings);
  dataSource.connector = driver;
  dataSource.connector.dataSource = dataSource;
  dataSource.connector.tableNameID = dataSource.settings.tableNameID;

  if (settings.lazyConnect) {
    process.nextTick(function() {
      callback();
    });
  } else {
    driver.connect(function(err, connection) {
      dataSource.client = connection;
      callback && callback(err, connection);
    });
  }
};

function MsSQL(settings) {
  MsSQL.super_.call(this, name, settings);
  // this.name = name;
  // this.settings = settings || {};
  this.settings.server = this.settings.host || this.settings.hostname;
  this.settings.user = this.settings.user || this.settings.username;
  // use url to override settings if url provided
  this.connConfig = this.settings.url || this.settings;
  this._models = {};
  this._idxNames = {};
}

util.inherits(MsSQL, SqlConnector);

MsSQL.newline = '\r\n';

/*!
 * This is a workaround to the limitation that 'msssql' driver doesn't support
 * parameterized SQL execution
 * @param {String} sql The SQL string with parameters as (?)
 * @param {*[]) params An array of parameter values
 * @returns {*} The fulfilled SQL string
 */
function format(sql, params) {
  if (Array.isArray(params)) {
    let count = 0;
    let index = -1;
    while (count < params.length) {
      index = sql.indexOf('(?)');
      if (index === -1) {
        return sql;
      }
      sql = sql.substring(0, index) + escape(params[count]) +
        sql.substring(index + 3);
      count++;
    }
  }
  return sql;
}

MsSQL.prototype.execute = function(sql, params, options, callback) {
  const lbc = LoopBackContext && LoopBackContext.getCurrentContext && LoopBackContext.getCurrentContext();
  if (typeof params === 'function' && options === undefined &&
    callback === undefined) {
    // execute(sql, callback)
    options = {};
    callback = params;
    params = [];
  } else if (typeof options === 'function' && callback === undefined) {
    // execute(sql, params, callback)
    callback = options;
    options = {};
  }
  params = params || [];
  options = options || {};
  if (typeof callback === 'function' && lbc && lbc.bind) {
    callback = lbc.bind(callback);
  }
  return SqlConnector.prototype.execute.call(this, sql, params, options, callback);
};

MsSQL.prototype.connect = function(callback) {
  const self = this;
  if (self.client) {
    return process.nextTick(callback);
  }
  const connection = new mssql.ConnectionPool(this.connConfig, function(err) {
    if (err) {
      debug('Connection error: ', err);
      return callback(err);
    }
    debug('Connection established: ', self.settings.server);
    connection.on('error', err => {
      debug('error: %j', err);
    });
    self.client = connection;
    callback(err, connection);
  });
};

function parameterizedSQL(sql) {
  let count = 0;
  let index = -1;
  while (true) {
    index = sql.indexOf('(?)');
    if (index === -1) {
      break;
    }
    count++;
    sql = sql.substring(0, index) + ('@param' + count) +
      sql.substring(index + 3);
  }
  return sql;
}

MsSQL.prototype.executeSQL = function(sql, params, options, callback) {
  debug('SQL: %s Parameters: %j', sql, params);

  // Convert (?) to @paramX
  sql = parameterizedSQL(sql);

  let connection = this.client;

  const transaction = options.transaction;
  if (transaction && transaction.connector === this && transaction.connection) {
    debug('Execute SQL in a transaction');
    connection = transaction.connection;
  }
  const innerCB = function(err, data) {
    debug('Result: %j %j', err, data);
    if (data) {
      data = data.recordset;
    }
    callback && callback(err, data);
  };

  const request = new mssql.Request(connection);
  // Allow multiple result sets
  if (options.multipleResultSets) {
    request.multiple = true;
  }

  if (Array.isArray(params) && params.length > 0) {
    for (let i = 0, n = params.length; i < n; i++) {
      if (typeof params[i] === 'number' &&
        params[i] % 1 !== 0) {
        // Float number
        request.input('param' + (i + 1), mssql.Real, params[i]);
      } else if (typeof params[i] === 'number' && isBigInt(params[i])) {
        request.input('param' + (i + 1), mssql.BigInt, params[i]);
      } else {
        request.input('param' + (i + 1), params[i]);
      }
    }
  }

  // request.verbose = true;
  request.query(sql, innerCB);
};

function isBigInt(num) {
  if (num > 2147483647 && num <= 9223372036854775807) return true;
  if (num < -2147483648 && num >= -9223372036854775808) return true;
  return false;
}

MsSQL.prototype.disconnect = function disconnect(cb) {
  this.client.close(cb);
};

// MsSQL.prototype.command = function (sql, callback) {
//     return this.execute(sql, callback);
// };

// params
// descr = {
//   model: ...
//   properties: ...
//   settings: ...
// }
MsSQL.prototype.define = function(modelDefinition) {
  if (!modelDefinition.settings) {
    modelDefinition.settings = {};
  }

  this._models[modelDefinition.model.modelName] = modelDefinition;

  // track database index names for this model
  this._idxNames[modelDefinition.model.modelName] = [];
};

MsSQL.prototype.getPlaceholderForValue = function(key) {
  return '@param' + key;
};

MsSQL.prototype.buildInsertDefaultValues = function(model, data, options) {
  return 'DEFAULT VALUES';
};

MsSQL.prototype.buildInsertInto = function(model, fields, options) {
  const stmt = this.invokeSuper('buildInsertInto', model, fields, options);
  const idName = this.idName(model);

  stmt.sql = idName ? (MsSQL.newline +
    'DECLARE @insertedIds TABLE (id ' + this.columnDataType(model, idName) + ')' +
    MsSQL.newline) + stmt.sql : stmt.sql;

  if (idName) {
    stmt.merge(
      'OUTPUT INSERTED.' +
      this.columnEscaped(model, idName) +
      ' into @insertedIds',
    );
  }
  return stmt;
};

MsSQL.prototype.buildInsert = function(model, data, options) {
  const idName = this.idName(model);
  const prop = this.getPropertyDefinition(model, idName);
  const isIdentity = (prop && prop.type === Number && prop.generated !== false);
  if (isIdentity && data[idName] == null) {
    // remove the pkid column if it's in the data, since we're going to insert a
    // new record, not update an existing one.
    delete data[idName];
    // delete the hardcoded id property that jugglindb automatically creates
    // delete data.id;
  }

  const stmt = this.invokeSuper('buildInsert', model, data, options);
  const tblName = this.tableEscaped(model);

  if (isIdentity && data[idName] != null) {
    stmt.sql = 'SET IDENTITY_INSERT ' + tblName + ' ON;' + MsSQL.newline +
      stmt.sql;
  }
  if (isIdentity && data[idName] != null) {
    stmt.sql += MsSQL.newline + 'SET IDENTITY_INSERT ' + tblName + ' OFF;' +
      MsSQL.newline;
  }
  if (idName) {
    stmt.sql += MsSQL.newline + 'SELECT id AS insertId from @insertedIds' + MsSQL.newline;
  }

  return stmt;
};

MsSQL.prototype.getInsertedId = function(model, info) {
  return info && info.length > 0 && info[0].insertId;
};

MsSQL.prototype.buildDelete = function(model, where, options) {
  const stmt = this.invokeSuper('buildDelete', model, where, options);
  stmt.merge(';SELECT @@ROWCOUNT as count', '');
  return stmt;
};

MsSQL.prototype.buildReplace = function(model, where, data, options) {
  const stmt = this.invokeSuper('buildReplace', model, where, data, options);
  stmt.merge(';SELECT @@ROWCOUNT as count', '');
  return stmt;
};

MsSQL.prototype.getCountForAffectedRows = function(model, info) {
  const affectedCountQueryResult = info && info[0];
  if (!affectedCountQueryResult) {
    return undefined;
  }
  const affectedCount = typeof affectedCountQueryResult.count === 'number' ?
    affectedCountQueryResult.count : undefined;
  return affectedCount;
};

MsSQL.prototype.buildUpdate = function(model, where, data, options) {
  const stmt = this.invokeSuper('buildUpdate', model, where, data, options);
  stmt.merge(';SELECT @@ROWCOUNT as count', '');
  return stmt;
};

// Convert to ISO8601 format YYYY-MM-DDThh:mm:ss[.mmm]
function dateToMsSql(val) {
  const dateStr = val.getUTCFullYear() + '-' +
    fillZeros(val.getUTCMonth() + 1) + '-' +
    fillZeros(val.getUTCDate()) +
    'T' + fillZeros(val.getUTCHours()) + ':' +
    fillZeros(val.getUTCMinutes()) + ':' +
    fillZeros(val.getUTCSeconds()) + '.';

  let ms = val.getUTCMilliseconds();
  if (ms < 10) {
    ms = '00' + ms;
  } else if (ms < 100) {
    ms = '0' + ms;
  } else {
    ms = '' + ms;
  }
  return dateStr + ms;

  function fillZeros(v) {
    return v < 10 ? '0' + v : v;
  }
}

function escape(val) {
  if (val === undefined || val === null) {
    return 'NULL';
  }

  switch (typeof val) {
    case 'boolean':
      return (val) ? 1 : 0;
    case 'number':
      return val + '';
  }

  if (typeof val === 'object') {
    val = (typeof val.toISOString === 'function') ?
      val.toISOString() :
      val.toString();
  }

  val = val.replace(/[\0\n\r\b\t\\\'\"\x1a]/g, function(s) {
    switch (s) {
      case '\0':
        return '\\0';
      case '\n':
        return '\\n';
      case '\r':
        return '\\r';
      case '\b':
        return '\\b';
      case '\t':
        return '\\t';
      case '\x1a':
        return '\\Z';
      case "\'":
        return "''"; // For sql server, double quote
      case '"':
        return s; // For oracle
      default:
        return '\\' + s;
    }
  });
  // return "q'#"+val+"#'";
  return "N'" + val + "'";
}

MsSQL.prototype.toColumnValue = function(prop, val) {
  if (val == null) {
    return null;
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      // Map NaN to NULL
      return val;
    }
    return val;
  }

  if (prop.type === Date || prop.type.name === 'Timestamp') {
    if (!val.toUTCString) {
      val = new Date(val);
    }
    val = dateToMsSql(val);
    return val;
  }

  if (prop.type === Boolean) {
    if (val) {
      return true;
    } else {
      return false;
    }
  }

  return this.serializeObject(val);
};

MsSQL.prototype.fromColumnValue = function(prop, val) {
  if (val == null) {
    return val;
  }
  const type = prop && prop.type;
  if (type === Boolean) {
    val = !!val; // convert to a boolean type from number
  }
  if (type === Date) {
    if (!(val instanceof Date)) {
      val = new Date(val.toString());
    }
  }
  return val;
};

MsSQL.prototype.escapeName = function(name) {
  return '[' + name.replace(/\./g, '_') + ']';
};

MsSQL.prototype.getDefaultSchemaName = function() {
  return 'dbo';
};

MsSQL.prototype.tableEscaped = function(model) {
  return this.escapeName(this.schema(model)) + '.' +
    this.escapeName(this.table(model));
};

MsSQL.prototype.applySqlChanges = function(model, pendingChanges, cb) {
  const self = this;
  if (pendingChanges.length) {
    const alterTable = (pendingChanges[0].substring(0, 10) !==
    'DROP INDEX' && pendingChanges[0].substring(0, 6) !== 'CREATE');
    let thisQuery = alterTable ? 'ALTER TABLE ' + self.tableEscaped(model) : '';
    let ranOnce = false;
    pendingChanges.forEach(function(change) {
      if (ranOnce) {
        thisQuery = thisQuery + ' ';
      }
      thisQuery = thisQuery + ' ' + change;
      ranOnce = true;
    });
    self.execute(thisQuery, cb);
  }
};

function buildLimit(limit, offset) {
  if (isNaN(offset)) {
    offset = 0;
  }
  let sql = 'ORDER BY RowNum OFFSET ' + offset + ' ROWS';
  if (limit >= 0) {
    sql += ' FETCH NEXT ' + limit + ' ROWS ONLY';
  }
  return sql;
}

MsSQL.prototype.buildColumnNames = function(model, filter, options) {
  let columnNames = this.invokeSuper('buildColumnNames', model, filter);
  if (filter.limit || filter.offset || filter.skip) {
    const orderBy = this.buildOrderBy(model, filter.order);
    let orderClause = '';
    let partitionByClause = '';
    if (options && options.partitionBy) {
      partitionByClause = 'PARTITION BY ' + this.columnEscaped(model, options.partitionBy);
    }
    if (orderBy) {
      orderClause = 'OVER (' + partitionByClause + ' ' + orderBy + ') ';
    } else {
      orderClause = 'OVER (' + partitionByClause + ' ' + 'ORDER BY (SELECT 1)) ';
    }
    columnNames += ',ROW_NUMBER() ' + orderClause + 'AS RowNum';
  }
  return columnNames;
};

MsSQL.prototype.buildSelect = function(model, filter, options) {
  if (!filter.order) {
    const idNames = this.idNames(model);
    if (idNames && idNames.length) {
      filter.order = idNames;
    }
  }

  let selectStmt = new ParameterizedSQL('SELECT ' +
    this.buildColumnNames(model, filter, options) +
    ' FROM ' + this.tableEscaped(model));

  if (filter) {
    if (filter.where) {
      const whereStmt = this.buildWhere(model, filter.where);
      selectStmt.merge(whereStmt);
    }

    if (filter.limit || filter.skip || filter.offset) {
      selectStmt = this.applyPagination(
        model, selectStmt, filter,
      );
    } else {
      if (filter.order) {
        selectStmt.merge(this.buildOrderBy(model, filter.order));
      }
    }
  }
  return this.parameterize(selectStmt);
};

MsSQL.prototype.applyPagination =
  function(model, stmt, filter) {
    const offset = filter.offset || filter.skip || 0;
    if (this.settings.supportsOffsetFetch) {
      // SQL 2012 or later
      // http://technet.microsoft.com/en-us/library/gg699618.aspx
      const limitClause = buildLimit(filter.limit, filter.offset || filter.skip);
      return stmt.merge(limitClause);
    } else {
      // SQL 2005/2008
      // http://blog.sqlauthority.com/2013/04/14/sql-server-tricks-for-row-offset-and-paging-in-various-versions-of-sql-server/
      let paginatedSQL = 'SELECT * FROM (' + stmt.sql + MsSQL.newline +
        ') AS S' + MsSQL.newline + ' WHERE S.RowNum > ' + offset;

      if (filter.limit !== -1) {
        paginatedSQL += ' AND S.RowNum <= ' + (offset + filter.limit);
      }

      stmt.sql = paginatedSQL + MsSQL.newline;
      return stmt;
    }
  };

MsSQL.prototype.buildExpression = function(columnName, operator, operatorValue,
  propertyDefinition) {
  switch (operator) {
    case 'like':
      return new ParameterizedSQL(columnName + " LIKE ? ESCAPE '\\'",
        [operatorValue]);
    case 'nlike':
      return new ParameterizedSQL(columnName + " NOT LIKE ? ESCAPE '\\'",
        [operatorValue]);
    case 'regexp':
      g.warn('{{Microsoft SQL Server}} does not support the regular ' +
          'expression operator');
    default:
      // invoke the base implementation of `buildExpression`
      return this.invokeSuper('buildExpression', columnName, operator,
        operatorValue, propertyDefinition);
  }
};

MsSQL.prototype.ping = function(cb) {
  this.execute('SELECT 1 AS result', cb);
};

require('./discovery')(MsSQL, mssql);
require('./migration')(MsSQL, mssql);
require('./transaction')(MsSQL, mssql);
