'use strict';
var _ = require('lodash');
var wait = true;
var config = require('./config');

function cl() {
  return console.log.apply(console, arguments);
}

function stringify(obj) {
  return JSON.stringify(obj, null, ' ');
}

function stopWaiting(fn) {
  return function() {
    if (fn) {
      fn.apply(null, arguments);
    }

    wait = false;
  };
}


var finish = stopWaiting(_.compose(cl, stringify));

function run(promise) {
  promise
    .then(finish)
    .catch(finish);
}

function doWait() {
  if (wait) {
    setTimeout(doWait, 10);
  }
}

function enforceSafety(argv) {
  if (!(argv.given('unsafe') || argv.given('u') || /http[s]?:\/\/0\.0\.0\.0(:\d+)?/.test(config.endpoint))) {
    throw new Error('Connect to a local DynamoDB instance or enable --unsafe mode and face the consequences.');
  }
}

function safe(fn) {
  var self = this;
  return function() {
    var n = arguments.length;
    if (n === 2) {
      enforceSafety(arguments[0]);
    } else if (n === 3) {
      enforceSafety(arguments[1]);
    } else {
      throw new Error('Unsupported case, cannot guarantee safety.');
    }
    return fn.apply(self, arguments);
  };
}


function scanCommand(argv, dbScanner) {
  if (argv.params.scan === true) {
    run(dbScanner.scanAllTables());
  } else {
    run(dbScanner.scanTable(argv.params.scan));
  }
}

function listCommand(argv, dbScanner) {
  run(dbScanner.listTables());
}

function schemaCommand(argv, dbScanner) {
  run(dbScanner.getTableSchema(argv.params.schema));
}

function describeTableCommand(argv, dbScanner) {
  run(dbScanner.describeTable(argv.params.describe));
}

function describeAllTablesCommand(argv, dbScanner) {
  run(dbScanner.describeAllTables());
}

function createTableCommand(descriptions, argv, dbScanner) {
  run(dbScanner.createTable(descriptions));
}

function createManyTablesCommand(descriptions, argv, dbScanner) {
  run(dbScanner.createManyTables(descriptions));
}

function deleteCommand(argv, dbScanner) {
  run(dbScanner.deleteTable(argv.params.delete));
}

function deleteAllCommand(argv, dbScanner) {
  run(dbScanner.deleteAllTables());
}

function createSnapshotCommand(argv, dbScanner) {
  run(dbScanner.createSnapshot());
}

function getItemCommand(args, argv, dbScanner) {
  run(dbScanner.getItem(args[0], args[1]));
}

function putItemCommand(args, argv, dbScanner) {
  run(dbScanner.putItem(args[0], args[1]));
}

function putMultipleItemsCommand(args, argv, dbScanner) {
  run(dbScanner.putMultipleItems(args[0], args[1]));
}

function updateAllInTableCommand(updateInput, argv, dbScanner) {
  run(dbScanner.updateAllInTable(updateInput));
}

function updateAllInManyTablesCommand( /* updateInput, argv, dbScanner */ ) {
  throw new Error('Multiple tables not supported yet!');
}

function updateItemCommand(updateInput, argv, dbScanner) {
  run(dbScanner.updateItem(updateInput.TableName, updateInput));
}

function updateManyItemsCommand(updateInput, argv, dbScanner) {
  run(dbScanner.updateMultipleItems(updateInput));
}

function recreateCommand(snapshot, argv, dbScanner) {
  run(dbScanner.deleteAllTables()
    .then(
      _.partial(
        dbScanner.recreateFromSnapshot,
        snapshot)));
}

function noopCommand() {
  wait = false;
}

module.exports = {
  wait: doWait,
  commands: {
    scan: scanCommand,
    list: listCommand,
    schema: schemaCommand,
    describe: {
      one: describeTableCommand,
      all: describeAllTablesCommand,
    },
    createTable: {
      one: safe(createTableCommand),
      many: safe(createManyTablesCommand),
    },
    delete: {
      one: safe(deleteCommand),
      all: safe(deleteAllCommand)
    },
    snapshot: createSnapshotCommand,
    getItem: getItemCommand,
    putItem: {
      one: safe(putItemCommand),
      many: safe(putMultipleItemsCommand)
    },
    update: {
      one: updateItemCommand,
      many: updateManyItemsCommand,
      allIn: {
        oneTable: safe(updateAllInTableCommand),
        manyTables: safe(updateAllInManyTablesCommand)
      }
    },
    recreate: safe(recreateCommand),
    noop: noopCommand
  }
};
