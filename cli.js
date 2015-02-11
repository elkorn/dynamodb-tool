#!/usr/bin/env node

'use strict';

var _ = require('lodash');
var wait = true;
var argv = require('./lib/util/argv');
var log = require('./lib/util/log');
var modules = require('./lib/main-module-provider');
var config = require('./lib/util/config');

function stopWaiting(fn) {
    return function() {
        if (fn) {
            fn.apply(null, arguments);
        }

        wait = false;
    };
}

function doWait() {
    if (wait) {
        setTimeout(doWait, 10);
    }
}

function cl() {
    return console.log.apply(console, arguments);
}

function stringify(obj) {
    return JSON.stringify(obj, null, ' ');
}


function enforceSafety() {
    if (!(argv.given('unsafe') || argv.given('u') || /http[s]?:\/\/0\.0\.0\.0(:\d+)?/.test(config.endpoint))) {
        throw new Error("Connect to local DynamoDB instance or enable --unsafe mode and face the consequences.");
    }
}

var finish = stopWaiting(_.compose(cl, stringify));

function run(promise) {
    promise
        .then(finish)
        .catch(finish);
}

function parseItemArguments(argv) {
    if (typeof(argv) !== 'string') {
        throw new Error('The argument for item operations must be a string.');
    }

    var args = argv.paramssplit(/=/);
    console.log(args);
    try {
        args[1] = JSON.parse(args[1]);
    } catch (e) {
        if (typeof(args[1]) !== 'string') {
            throw new Error('Invalid item descriptor provided.');
        }

        args[1] = require(args[1]);
    }

    log.verbose(args);
    return args;
}

var dbScanner = modules.makeDbScanner();

switch (true) {
    case argv.given('scan'):
        if (argv.params.scan === true) {
            run(dbScanner.scanAllTables());
        } else {
            run(dbScanner.scanTable(argv.params.scan));
        }

        break;
    case argv.given('list'):
        log.verbose('listing tables...');
        run(dbScanner.listTables());
        break;
    case argv.given('schema'):
        log.verbose('getting DB schema...');
        run(dbScanner.getTableSchema(argv.params.schema));
        break;
    case argv.given('describe'):
        if (typeof(argv.params.describe) === 'string') {
            log.verbose('describing a table...');
            run(dbScanner.describeTable(argv.params.describe));
        } else {
            log.verbose('describing all tables...');
            run(dbScanner.describeAllTables());
        }
        break;
    case argv.given('create'):
        enforceSafety();
        var descriptions = require(argv.params.create);
        if (_.isArray(descriptions)) {
            log.verbose('creating multiple tables...');
            run(dbScanner.createManyTables(descriptions));
        } else {
            log.verbose('creating a table...');
            run(dbScanner.createTable(descriptions));
        }
        break;
    case argv.given('delete'):
        enforceSafety();
        log.verbose('deleting a table...');
        run(dbScanner.deleteTable(argv.params.delete));
        break;
    case argv.given('delete-all'):
        enforceSafety();
        log.verbose('deleting all tables...');
        run(dbScanner.deleteAllTables());
        break;
    case argv.given('snapshot'):
        log.say('creating a snapshot...');
        run(dbScanner.createSnapshot());
        break;
    case argv.given('get'):
        log.verbose('getting item...');
        var args = parseItemArguments(argv.params.get);
        run(dbScanner.getItem(args[0], args[1]));
        break;
    case argv.given('put'):
        enforceSafety();
        var args = parseItemArguments(argv.params.put);
        if (_.isArray(args[1])) {
            log.verbose('putting mutliple items...');
            run(dbScanner.putMultipleItems(args[0], args[1]));
        } else {
            log.verbose('putting single item...');
            run(dbScanner.putItem(args[0], args[1]));
        }

        break;
    case argv.given('update-all'):
        enforceSafety();
        var updateInput;
        try {
            updateInput = JSON.parse(argv['update-all']);
        } catch (e) {
            updateInput = require(argv['update-all']);
        }

        var isArray = _.isArray(updateInput);

        if (isArray) {
            throw new Error('Multiple tables not supported yet!');
        } else {
            log.verbose(require('util').format('updating all items in table %s...', updateInput.TableName));
            run(dbScanner.updateAllInTable(updateInput));
        }

        break;

    case argv.given('update'):
        enforceSafety();
        var updateInput;
        try {
            updateInput = JSON.parse(argv.params.update);
        } catch (e) {
            updateInput = require(argv.params.update);
        }

        var isArray = _.isArray(updateInput);

        if (isArray) {
            log.verbose('updating multiple items...');
            run(dbScanner.updateMultipleItems(updateInput));
        } else {
            log.verbose('updating single item...');
            run(dbScanner.updateItem(updateInput.TableName, updateInput));
        }

        break;
    case argv.given('recreate'):
        enforceSafety();
        var snapshot;
        try {
            snapshot = JSON.parse(argv.params.recreate);
        } catch (e) {
            snapshot = require(argv.params.recreate);
        }

        log.say('recreating database...');

        run(dbScanner.deleteAllTables()
            .then(
                _.partial(
                    dbScanner.recreateFromSnapshot,
                    snapshot)));
        break;
    default:
        wait = false;
}

doWait();
