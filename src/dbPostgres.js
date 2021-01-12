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
  let logger;

  /**
   * Initializes the connection pool for the target database.
   * See https://github.com/brianc/node-postgres
   *
   * @param config - A configuration object to be passed to pg.Pool.
   */
  const init = (config) => {
    logger = winston.loggers.get('app');
    pool = new pg.Pool(config);
  };

  const query = ({ q, p = [] }, callback) => {
    logger.debug('Postgres Query', { q });
    pool.query(q, p, (err, res) => {
      if (err) {
        return callback(err);
      }
      return callback(null, res);
    });
  };

  const query2 = (q, callback) => {
    logger.debug('Postgres Query', { q });
    pool.query(q, (err, res) => {
      if (err) {
        return callback(err);
      }
      return callback(null, res);
    });
  };

  const runScriptFile = (path, callback) => {
    logger.debug('target.runScriptFile', { path });
    async.waterfall([
      async.constant(path), // , 'utf8'),
      fs.readFile,
      // Convert file content to string
      (content, cb) => { cb(null, content.toString()); },
      query2,
    ], callback);
  };

  const importFile = (table, filepath, callback) => {
    logger.debug('db_postgres.importFile', { table, filepath });

    const statement = `COPY ${table} FROM STDIN DELIMITER ',' CSV NULL AS '\\N' ENCODING 'LATIN1' ESCAPE '\\';`;

    logger.debug('Copy Statement', { statement });

    pool.connect((err, client, done) => {
      if (err) {
        logger.error('Pool connection error', { err });
        return callback(err);
      }
      const cf = copyFrom(statement);

      function allDone(streamErr) {
        if (streamErr) {
          logger.error('Copy From Error', { streamErr });
        }
        // Close the connection to the database
        done();
        callback(streamErr, cf.rowCount);
      }

      const stream = client.query(cf);
      const fileStream = fs.createReadStream(filepath);
      fileStream.on('error', allDone);
      stream.on('error', allDone);
      stream.on('finish', allDone);

      return fileStream.pipe(stream);
    });
  };

  return {
    init,
    query,
    runScriptFile,
    importFile,
  };
})();
