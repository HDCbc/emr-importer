const async = require('async');
const copyFrom = require('pg-copy-streams').from;
const fs = require('fs');
const pg = require('pg');
const winston = require('winston');

/**
 * This module handles all interactions with the target database.
 */
module.exports = (() => {
  // The current pool of connections.
  // This will not be instantiated until the init function is called.
  let pool;

  /**
   * Initializes the connection pool for the target database.
   * See https://github.com/brianc/node-postgres
   *
   * @param config - A configuration object to be passed to pg.Pool.
   */
  const init = (config) => {
    pool = new pg.Pool(config);
  };

  const query = ({ q, p = [] }, callback) => {
    winston.debug('Postgres Query', q);
    pool.query(q, p, (err, res) => {
      if (err) {
        return callback(err);
      }
      return callback(null, res);
    });
  };

  const query2 = (q, callback) => {
    winston.debug('Postgres Query', q);
    pool.query(q, (err, res) => {
      if (err) {
        return callback(err);
      }
      return callback(null, res);
    });
  };

  const runScriptFile = (path, callback) => {
    winston.debug('target.runScriptFile', path);
    async.waterfall([
      async.constant(path), // , 'utf8'),
      fs.readFile,
      function (huh, cb) { cb(null, huh.toString()); },
      query2,
    ], callback);
  };

  const importFile = (table, filepath, callback) => {
    winston.debug('db_postgres.importFile', { table, filepath });

    const statement = `COPY ${table} FROM STDIN DELIMITER ',' CSV NULL AS '\\N' ENCODING 'LATIN1' ESCAPE '\\';`;

    winston.debug('Copy Statement', statement);

    pool.connect((err, client, done) => {
      if (err) {
        return callback(err);
      }

      function allDone(a, b) {
        done();
        callback(a, b);
      }

      const stream = client.query(copyFrom(statement));
      const fileStream = fs.createReadStream(filepath);
      fileStream.on('error', allDone);
      stream.on('error', allDone);
      stream.on('end', allDone);
      fileStream.pipe(stream);
    });
  };

  return {
    init,
    query,
    runScriptFile,
    importFile,
  };
})();
