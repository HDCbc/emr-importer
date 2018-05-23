// Import npm modules
const _ = require('lodash');
const async = require('async');
const chokidar = require('chokidar');
const decompress = require('decompress');
const fs = require('fs');
const logger = require('winston');
const path = require('path');
const rimraf = require('rimraf');
const winston = require('winston');

// Import local modules
const dbPostgres = require('./dbPostgres');

const uploadData = (db, table, filepath, callback) => {
  db.importFile(table, filepath, (err, res) => callback(err, res));
};

const importTask = (db, table, filepath, callback) => {
  const start = Date.now();

  async.series({
    uploadData: async.apply(uploadData, db, table, filepath),
  }, (err, res) => {
    const elapsedSec = (Date.now() - start) / 1000;
    if (err) {
      winston.error(`THERE IS AN ERROR FOR THE TASK ${table} ${filepath}`, err);
      return callback(err);
    }

    // TODO - display row counts
    winston.verbose(`    CSV Uploaded ${path.basename(filepath)} (Some ### in ${elapsedSec} sec)`);
    return callback(err, res);
  });
};

const populateTasks = (dataDir, db, callback) => {
  winston.info('  Populating Tasks Started');

  const tasks = [];

  fs.readdir(dataDir, (err, files) => {
    if (files.length === 0) {
      return callback(`No files found in ${dataDir}`);
    }
    _.forEach(files, (filename) => {
      const filepath = path.join(dataDir, filename);
      let table;

      if (filename.startsWith('Clinic')) {
        table = 'etl.clinic';
      } else if (filename.startsWith('Practitioner')) {
        table = 'etl.practitioner';
      } else if (filename.startsWith('PatientPractitioner')) {
        table = 'etl.patient_practitioner';
      } else if (filename.startsWith('PatientState')) { // Must be after patient-practitioner
        table = 'etl.patient_state';
      } else if (filename.startsWith('Patient')) { // Must be after patient-practitioner and patient-state
        table = 'etl.patient';
      } else if (filename.startsWith('EntryAttribute')) {
        table = 'etl.entry_attribute';
      } else if (filename.startsWith('EntryState')) {
        table = 'etl.entry_state';
      } else if (filename.startsWith('Entry')) { // Must be after entry-attribute and state
        table = 'etl.entry';
      } else {
        winston.warn('Skipping File', { filename });
      }

      if (table) {
        tasks.push(async.apply(importTask, db, table, filepath));
        winston.verbose('Task Created', { table, filepath });
      }
    });

    winston.info(`  Populating Tasks Completed (${Object.keys(tasks).length} tasks from ${files.length} files)`);
    return callback(null, tasks);
  });
};

function runTasks(tasks, parallelLimit, callback) {
  winston.info('Upload Started', { parallelLimit, tasks: tasks.length });
  const start = Date.now();

  async.parallelLimit(tasks, parallelLimit, (err, res) => {
    const elapsedSec = (Date.now() - start) / 1000;

    if (err) {
      return callback(err);
    }

    // const rows = _.sumBy(_.toArray(res), t => (t.rows ? t.rows : 0));

    winston.info(`  Upload Completed (${-42} rows in ${elapsedSec} sec)`);
    return callback(err, res);
  });
}

function uncompress(sourceFile, targetDir, callback) {
  winston.info('Decompress Started', { sourceFile, targetDir });

  const start = Date.now();

  decompress(sourceFile, targetDir)
    .then((files) => {
      const elapsedSec = (Date.now() - start) / 1000;
      winston.info(`  Decompress Completed (${elapsedSec} sec)`);
      return callback(null, files);
    })
    .catch((err) => {
      winston.error('Decompress Failed', { err });
      return callback(err);
    });
}

function runScriptFile(db, taskName, relativePath, callback) {
  const scriptPath = path.join(__dirname, relativePath);
  winston.info(`Script ${taskName} Started`, { taskName, scriptPath });
  const start = Date.now();

  db.runScriptFile(scriptPath, (err, res) => {
    if (err) {
      console.log('ERROR', err);
      winston.error(`Script ${taskName} Failed`, { err });
      return callback(err);
    }

    const elapsedSec = (Date.now() - start) / 1000;
    winston.info(`${taskName} Completed (${elapsedSec} sec)`);
    return callback(err, res);
  });
}

function processFile(db, filepath, workingDir, parallelImports, processedExt, callback) {
  winston.info('Import Started', { filepath, workingDir });
  const start = Date.now();

  return async.auto({
    // Clear working directory.
    clearWorking: (cb) => {
      const pattern = `${workingDir}/*`;
      winston.info('Start clear working', { pattern });
      rimraf(pattern, { }, (err) => {
        if (err) {
          winston.error('Unable to clear working', { err });
          return cb(err);
        }
        winston.info('Cleared Working');
        return cb(null);
      });
    },
    // Initialize the etl tables to import data into.
    etlSchema: ['clearWorking', (res, cb) => {
      runScriptFile(db, 'Recreate ETL Schema', '../sql/createEtl.sql', cb);
    }],
    // Truncate all clinical data.
    truncateUniversal: ['etlSchema', (res, cb) => {
      runScriptFile(db, 'Truncate Universal Schema Data', '../sql/truncateUniversal.sql', cb);
    }],
    // Uncompress the zipped file.
    uncompressFile: ['truncateUniversal', (res, cb) => {
      uncompress(filepath, workingDir, cb);
    }],
    // Populate a list of tasks based on the uncompressed filenames.
    populateTasks: ['uncompressFile', (res, cb) => {
      populateTasks(workingDir, db, cb);
    }],
    // Run the tasks (eg get the data from the files into the etl tables)
    runTasks: ['populateTasks', (res, cb) => {
      runTasks(res.populateTasks, parallelImports, cb);
    }],
    // Synchronize clinics
    syncClinic: ['runTasks', (res, cb) => {
      runScriptFile(db, 'Synchronize Clinics', '../sql/syncClinic.sql', cb);
    }],
    syncPatient: ['syncClinic', (res, cb) => {
      runScriptFile(db, 'Synchronize Patients', '../sql/syncPatient.sql', cb);
    }],
    syncPatientState: ['syncPatient', (res, cb) => {
      runScriptFile(db, 'Synchronize Patient State', '../sql/syncPatientState.sql', cb);
    }],
    syncPractitioner: ['syncPatientState', (res, cb) => {
      runScriptFile(db, 'Synchronize Practitioners', '../sql/syncPractitioner.sql', cb);
    }],
    syncPatientPractitioner: ['syncPractitioner', (res, cb) => {
      runScriptFile(db, 'Synchronize Patient Practitioners', '../sql/syncPatientPractitioner.sql', cb);
    }],
    syncEntry: ['syncPatientPractitioner', (res, cb) => {
      runScriptFile(db, 'Synchronize Entry', '../sql/syncEntry.sql', cb);
    }],
    syncEntryAttributePreUnlogged: ['syncEntry', (res, cb) => {
      runScriptFile(db, 'Synchronize Entry Attribute (Pre Unlogged)', '../sql/syncEntryAttributePreUnlogged.sql', cb);
    }],
    syncEntryAttributePreConstraints: ['syncEntryAttributePreUnlogged', (res, cb) => {
      runScriptFile(db, 'Synchronize Entry Attribute (Pre Constraints)', '../sql/syncEntryAttributePreConstraints.sql', cb);
    }],
    syncEntryAttributePreIndices: ['syncEntryAttributePreConstraints', (res, cb) => {
      runScriptFile(db, 'Synchronize Entry Attribute (Pre Indices)', '../sql/syncEntryAttributePreIndices.sql', cb);
    }],
    syncEntryAttribute: ['syncEntryAttributePreIndices', (res, cb) => {
      runScriptFile(db, 'Synchronize Entry Attribute', '../sql/syncEntryAttribute.sql', cb);
    }],
    syncEntryAttributePostLogged: ['syncEntryAttribute', (res, cb) => {
      runScriptFile(db, 'Synchronize Entry Attribute (Post Logged)', '../sql/syncEntryAttributePostLogged.sql', cb);
    }],
    syncEntryAttributePostConstraints: ['syncEntryAttributePostLogged', (res, cb) => {
      runScriptFile(db, 'Synchronize Entry Attribute (Post Constraints)', '../sql/syncEntryAttributePostConstraints.sql', cb);
    }],
    syncEntryAttributePostIndices: ['syncEntryAttributePostConstraints', (res, cb) => {
      runScriptFile(db, 'Synchronize Entry Attribute (Post Indices)', '../sql/syncEntryAttributePostIndices.sql', cb);
    }],
    syncEntryAttributePostAnalyze: ['syncEntryAttributePostIndices', (res, cb) => {
      runScriptFile(db, 'Synchronize Entry Attribute (Post Analyze)', '../sql/syncEntryAttributePostAnalyze.sql', cb);
    }],
    syncEntryState: ['syncEntryAttributePostAnalyze', (res, cb) => {
      runScriptFile(db, 'Synchronize Entry State', '../sql/syncEntryState.sql', cb);
    }],
    dropEtl: ['syncEntryState', (res, cb) => {
      runScriptFile(db, 'Drop ETL', '../sql/dropEtl.sql', cb);
    }],
    vacuum: ['dropEtl', (res, cb) => {
      runScriptFile(db, 'Vacuum', '../sql/vacuum.sql', cb);
    }],
    // Clear the working directory with the uncompressed files.
    removeTemp: ['vacuum', (res, cb) => {
      cb(null);
    }],
    // Rename the imported file to indicate that it was processed successfully.
    renameFile: ['removeTemp', (res, cb) => {
      const processedPath = path.join(path.dirname(filepath), `${path.basename(filepath)}.${processedExt}`);
      winston.info('Renaming file', { filepath, processedPath });
      fs.rename(filepath, processedPath, cb);
    }],
  }, (err) => {
    if (err) {
      console.log('Error', err);
      return callback(err);
    }
    const elapsedSec = (Date.now() - start) / 1000;
    winston.info(`Import Complete (${elapsedSec} sec)`);
    return callback(err);
  });
}

function waitForConnection(db, callback) {
  winston.info('Wait for Connection Started');
  const start = Date.now();

  const testConnection = cb => db.query({ q: 'select 1' }, cb);

  async.retry({
    times: 60,
    internal: 1000,
    errorFilter(err) {
      winston.info(`  ${err}`);
      return true;
    },
  }, testConnection, (err) => {
    if (err) {
      return callback(err);
    }

    const elapsedSec = (Date.now() - start) / 1000;
    winston.info(`Wait for Connection Completed (${elapsedSec} sec)`);
    return callback(null);
  });
}

function startWatching(watchDir, ignoreExt, queue) {
  logger.info('Watching Started', { watchDir, ignoreExt });

  // Full list of options. See below for descriptions. (do not use this example)
  const watcher = chokidar.watch(watchDir, {
    persistent: true,
    ignored: (filepath) => {
      return path.extname(filepath) === `.${ignoreExt}`;
    },
    ignoreInitial: false,
    alwaysStat: true,
    awaitWriteFinish: {
      stabilityThreshold: 2000,
      pollInterval: 100,
    },
  });

  watcher.on('add', (filepath) => {
    logger.info('File Detected', { filepath });
    queue.push(filepath);
  });
}

/**
 * Run the importer.
 *
 */
function run(options) {
  const start = Date.now();
  logger.info(_.repeat('=', 160));
  logger.info('Run Started');

  // It is assumed that the options have been verified in the config module.
  const {
    sourceDir,
    processedExt,
    target,
    parallelImports,
    workingDir,
  } = options;

  const db = dbPostgres;
  db.init(target);
  console.log('db', db);
  // Mask the password before logging.
  const logOptions = Object.assign({}, options, {
    target: Object.assign({}, options.target, { password: 'XXX' }),
  });
  logger.verbose('Configuration');
  _.forEach(_.keys(logOptions), (key) => {
    logger.verbose(`-${key}`, { value: logOptions[key] });
  });

  // Create a queue object with concurrency of 1. When files are added to the queue they will
  // be processed one at a time.
  const queue = async.queue((filepath, cb) => {
    logger.verbose('File Queued', filepath);
    processFile(db, filepath, workingDir, parallelImports, processedExt, cb);
  }, 1);

  // This function keeps the application running as long as it continues to watch for files.
  startWatching(sourceDir, processedExt, queue);
}

// Reveal the public functions
module.exports = {
  run,
};
