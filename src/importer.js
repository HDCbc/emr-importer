// Import npm modules
const _ = require('lodash');
const async = require('async');
const { spawnSync } = require('child_process');
const chokidar = require('chokidar');
const fs = require('fs');
const path = require('path');
const replaceStream = require('replacestream');
const rimraf = require('rimraf');
const winston = require('winston');

// Import local modules
const dbPostgres = require('./dbPostgres');

let logger;

const convertFileData = (filepath, callback) => {
  const start = Date.now();
  const filename = path.basename(filepath);
  logger.verbose('Removing Bad Dates Started', { filename });

  // Note that all of the regex streams must be done in separate streams, otherwise if a chunk of
  // data is being compared to the regex, and that chunk matches one of the lazier matches it will,
  // even though it might have matched a longer regex if it had read in the next chunk. We want all
  // of the separate regex to be greedy. The replacestream module deals with regex across chunks,
  // but does not appear to work as needed with multiple regex in one stream where the order matters
  const readStream = fs.createReadStream(filepath, { encoding: 'utf8', autoClose: true });
  const replacerStream1 = replaceStream(/"0000-00-00 00:00:00"/g, '\\N');
  const replacerStream2 = replaceStream(/0000-00-00 00:00:00/g, '\\N');
  const replacerStream3 = replaceStream(/"0000-00-00T00:00:00.000000Z"/g, '\\N');
  const replacerStream4 = replaceStream(/0000-00-00T00:00:00.000000Z/g, '\\N');
  const replacerStream5 = replaceStream(/"0000-00-00"/g, '\\N');
  const replacerStream6 = replaceStream(/0000-00-00/g, '\\N');

  const writeStream = fs.createWriteStream(`${filepath}.tmp`, { encoding: 'utf8', autoClose: true });

  readStream.on('error', (readErr) => {
    logger.error(`Removing Bad Date Read Error (${filename})`, { readErr });
    return callback(readErr);
  });

  const replaceError = (replaceErr) => {
    logger.error(`Removing Bad Date Replace Error (${filename})`, { replaceErr });
    return callback(replaceErr);
  };

  replacerStream1.on('error', replaceError);
  replacerStream2.on('error', replaceError);
  replacerStream3.on('error', replaceError);
  replacerStream4.on('error', replaceError);
  replacerStream5.on('error', replaceError);
  replacerStream6.on('error', replaceError);

  writeStream.on('error', (writeErr) => {
    logger.error(`Removing Bad Date Write Error (${filename})`, { writeErr });
    return callback(writeErr);
  });

  readStream
    .pipe(replacerStream1)
    .pipe(replacerStream2)
    .pipe(replacerStream3)
    .pipe(replacerStream4)
    .pipe(replacerStream5)
    .pipe(replacerStream6)
    .pipe(writeStream);

  writeStream.on('finish', async () => {
    fs.rename(`${filepath}.tmp`, filepath, (renameErr) => {
      if (renameErr) {
        logger.error(`Removing Bad Date Rename Error (${filename})`, { renameErr });
        return callback(renameErr);
      }

      const elapsedSec = (Date.now() - start) / 1000;
      logger.verbose('Removing Bad Dates Success', { filename, elapsedSec });
      return callback(null);
    });
  });
};

const uploadData = (db, table, filepath, callback) => {
  const start = Date.now();
  const filename = path.basename(filepath);

  logger.debug(' CSV Upload Started', { filename });

  convertFileData(filepath, (convertErr) => {
    if (convertErr) {
      logger.error(` CSV Upload Convert Error (${table} ${filepath})`, { convertErr });
      return callback(convertErr);
    }

    return db.importFile(table, filepath, (importErr, rowCount) => {
      if (importErr) {
        logger.error(`CSV Upload Import Error (${table} ${filepath})`, { importErr });
        return callback(importErr);
      }

      const elapsedSec = (Date.now() - start) / 1000;
      logger.verbose(` CSV Uploaded ${filename} (${rowCount} rows in ${elapsedSec} sec)`);
      return callback(importErr, { rowCount, elapsedSec });
    });
  });
};

const populateTasks = (dataDir, db, callback) => {
  logger.info('Populating Tasks Started');

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
        logger.warn('Skipping File', { filename });
      }

      if (table) {
        tasks.push(async.apply(uploadData, db, table, filepath));
        logger.verbose(' Task Created', { table, filepath });
      }
    });

    logger.info(`Populating Tasks Completed (${Object.keys(tasks).length} tasks from ${files.length} files)`);
    return callback(null, tasks);
  });
};

function runTasks(tasks, parallelLimit, callback) {
  logger.info('Uploading Started', { parallelLimit, tasks: tasks.length });
  const start = Date.now();

  async.parallelLimit(tasks, parallelLimit, (err, res) => {
    const elapsedSec = (Date.now() - start) / 1000;

    if (err) {
      return callback(err);
    }

    const rowCount = _.sumBy(_.toArray(res), (t) => (t.rowCount ? t.rowCount : 0));
    const serialElapsedSec = _.sumBy(_.toArray(res), (t) => (t.elapsedSec ? t.elapsedSec : 0));
    const serialTruncated = (Math.floor(serialElapsedSec) * 1000) / 1000;

    logger.info(`Uploading Completed (${rowCount} rows in ${elapsedSec} sec, serial ${serialTruncated} sec)`);
    return callback(err, res);
  });
}

function uncompress(sourceFile, targetDir, callback) {
  logger.info('Decompress Started', { sourceFile, targetDir });

  const start = Date.now();

  if (!fs.existsSync(sourceFile)) {
    const err = `Cannot decompress file '${sourceFile}' that does not exist`;
    logger.error(err);
    return callback(err);
  }

  logger.debug('Decompress source exists', { sourceFile });

  if (!fs.existsSync(targetDir)) {
    const err = `Cannot decompress to directory '${targetDir}' that does not exist`;
    logger.error(err);
    return callback(err);
  }

  logger.debug('Decompress target exists', { targetDir });

  const { stdout, stderr, status } = spawnSync('unzip', ['-d', targetDir, sourceFile]);

  logger.debug('Process Stdout', stdout);

  if(stderr && stderr.toString().trim() !== '') {
    logger.error('Process Stderr', stderr);
  }

  if (status === 0) {
    const elapsedSec = (Date.now() - start) / 1000;
    logger.info(`Decompress Completed (${elapsedSec} sec)`);
    return callback(null);
  }

  logger.error('Decompress Failed', { status });
  return callback(`Decompress return code ${status}`);
}

function runScriptFile(db, taskName, relativePath, callback) {
  const scriptPath = path.join(__dirname, relativePath);
  logger.info(`Script ${taskName} Started`, { taskName, scriptPath });
  const start = Date.now();

  db.runScriptFile(scriptPath, (err, res) => {
    if (err) {
      logger.error(`Script ${taskName} Failed`, { err });
      return callback(err);
    }

    const elapsedSec = (Date.now() - start) / 1000;
    logger.info(`Script ${taskName} Completed (${elapsedSec} sec)`);
    return callback(err, res);
  });
}

function deleteFolder(filepath, callback) {
  logger.info('Delete Folder Started', { filepath });
  const start = Date.now();

  rimraf(filepath, {}, (err) => {
    if (err) {
      logger.error(`Delete folder (${filepath}) Failed`, { err });
      return callback(err);
    }

    const elapsedSec = (Date.now() - start) / 1000;
    logger.info(`Delete Folder Completed  (${elapsedSec} sec)`);
    return callback(null);
  });
}

function renameFile(fromPath, toPath, callback) {
  logger.info('Rename File Started', { fromPath, toPath });
  const start = Date.now();

  fs.rename(fromPath, toPath, (err) => {
    if (err) {
      logger.error(`Rename File (${fromPath}) Failed`, { err });
      return callback(err);
    }

    const elapsedSec = (Date.now() - start) / 1000;
    logger.info(`Rename File Completed  (${elapsedSec} sec)`);
    return callback(null);
  });
}

function processFile(db, filepath, workingDir, parallelImports, processedExt, callback) {
  logger.info('Import Started', { filepath, workingDir });
  const start = Date.now();

  return async.auto({
    // Clear working directory.
    clearWorking: (cb) => {
      deleteFolder(`${workingDir}/*`, cb);
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
    logImport: ['dropEtl', (res, cb) => {
      runScriptFile(db, 'Log Import', '../sql/logImport.sql', cb);
    }],
    vacuum: ['logImport', (res, cb) => {
      runScriptFile(db, 'Vacuum', '../sql/vacuum.sql', cb);
    }],
    // Clear the working directory with the uncompressed files.
    removeTemp: ['vacuum', (res, cb) => {
      cb(null);
    }],
    // Rename the imported file to indicate that it was processed successfully.
    renameFile: ['removeTemp', (res, cb) => {
      const processedPath = path.join(path.dirname(filepath), `${path.basename(filepath)}.${processedExt}`);
      renameFile(filepath, processedPath, cb);
    }],
  }, (err) => {
    if (err) {
      logger.error('Import Error', { err });
      return callback(err);
    }
    const elapsedSec = (Date.now() - start) / 1000;
    logger.info(`Import Complete (${elapsedSec} sec)`);
    return callback(err);
  });
}

function startWatching(watchDir, ignoreExt, queue) {
  logger.info('Watching Started', { watchDir, ignoreExt });

  // Full list of options. See below for descriptions. (do not use this example)
  const watcher = chokidar.watch(watchDir, {
    persistent: true,
    ignored: (filepath) => path.extname(filepath) === `.${ignoreExt}`,
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
  logger = winston.loggers.get('app');
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

  // Mask the password before logging.
  const logOptions = { ...options, target: { ...options.target, password: 'XXX' } };
  logger.verbose('Configuration');
  _.forEach(_.keys(logOptions), (key) => {
    logger.verbose(`-${key}`, { value: logOptions[key] });
  });

  // Create a queue object with concurrency of 1. When files are added to the queue they will
  // be processed one at a time.
  const queue = async.queue((filepath, cb) => {
    logger.verbose('File Queued', { filepath });
    processFile(db, filepath, workingDir, parallelImports, processedExt, cb);
  }, 1);

  // This function keeps the application running as long as it continues to watch for files.
  startWatching(sourceDir, processedExt, queue);
}

// Reveal the public functions
module.exports = {
  run,
};
