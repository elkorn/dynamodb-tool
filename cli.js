#! /usr/bin/env node

'use strict';

var _ = require('lodash');
var argv = require('minimist')(process.argv.slice(2));
var config = require(argv.config || './config');

var wait = true;

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

function log() {
    return console.log.apply(console, arguments);
}

function stringify(obj) {
    return JSON.stringify(obj, null, ' ');
}

function givenArg(val) {
    return argv.hasOwnProperty(val);
}

function enforceSafety() {
    if (!(givenArg('unsafe') || givenArg('u') || /http[s]?:\/\/0\.0\.0\.0(:\d+)?/.test(config.endpoint))) {
        throw new Error("Connect to local DynamoDB instance or enable --unsafe mode and face the consequences.");
    }
}

var finish = stopWaiting(_.compose(log, stringify));

function say(msg) {
    process.stderr.write(msg + '\n');
}

function verbose(msg) {
    if (config.verbose) {
        say(msg);
    }
}

function makeDynamo() {
    var AWS = require('aws-sdk');
    var result = new AWS.DynamoDB({
        region: config.region || AWS.config.region || 'eu-west-1',
        endpoint: config.endpoint || AWS.config.endpoint
    });

    verbose('\nEndpoint: ' + result.endpoint.host + '\n');
    return result;
}

function makeDbScanner() {
    return new(require('./lib/db-scanner')).DBScanner(makeDynamo());
}

function run(promise) {
    promise
        .then(finish)
        .catch(finish);
}

function parseItemArguments(argv) {
    if (typeof(argv) !== 'string') {
        throw new Error('The argument for item operations must be a string.');
    }

    var args = argv.split(/=/);
    console.log(args);
    try {
        args[1] = JSON.parse(args[1]);
    } catch (e) {
        if (typeof(args[1]) !== 'string') {
            throw new Error('Invalid item descriptor provided.');
        }

        args[1] = require(args[1]);
    }

    verbose(args);
    return args;
}

var dbScanner = makeDbScanner();

switch (true) {
    case givenArg('scan'):
        if (argv.scan === true) {
            run(dbScanner.scanAllTables());
        } else {
            run(dbScanner.scanTable(argv.scan));
        }

        break;
    case givenArg('list'):
        verbose('listing tables...');
        run(dbScanner.listTables());
        break;
    case givenArg('schema'):
        verbose('getting DB schema...');
        run(dbScanner.getTableSchema(argv.schema));
        break;
    case givenArg('describe'):
        if (typeof(argv.describe) === 'string') {
            verbose('describing a table...');
            run(dbScanner.describeTable(argv.describe));
        } else {
            verbose('describing all tables...');
            run(dbScanner.describeAllTables());
        }
        break;
    case givenArg('create'):
        enforceSafety();
        var descriptions = require(argv.create);
        if (_.isArray(descriptions)) {
            verbose('creating multiple tables...');
            run(dbScanner.createManyTables(descriptions));
        } else {
            verbose('creating a table...');
            run(dbScanner.createTable(descriptions));
        }
        break;
    case givenArg('delete'):
        enforceSafety();
        verbose('deleting a table...');
        run(dbScanner.deleteTable(argv.delete));
        break;
    case givenArg('delete-all'):
        enforceSafety();
        verbose('deleting all tables...');
        run(dbScanner.deleteAllTables());
        break;
    case givenArg('snapshot'):
        say('creating a snapshot...');
        run(dbScanner.createSnapshot());
        break;
    case givenArg('get'):
        verbose('getting item...');
        var args = parseItemArguments(argv.get);
        run(dbScanner.getItem(args[0], args[1]));
        break;
    case givenArg('put'):
        enforceSafety();
        var args = parseItemArguments(argv.put);
        if (_.isArray(args[1])) {
            verbose('putting mutliple items...');
            run(dbScanner.putMultipleItems(args[0], args[1]));
        } else {
            verbose('putting single item...');
            run(dbScanner.putItem(args[0], args[1]));
        }

        break;
    case givenArg('update-all'):
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
            verbose(require('util').format('updating all items in table %s...', updateInput.TableName));
            run(dbScanner.updateAllInTable(updateInput));
        }

        break;

    case givenArg('update'):
        enforceSafety();
        var updateInput;
        try {
            updateInput = JSON.parse(argv.update);
        } catch (e) {
            updateInput = require(argv.update);
        }

        var isArray = _.isArray(updateInput);

        if (isArray) {
            verbose('updating multiple items...');
            run(dbScanner.updateMultipleItems(updateInput));
        } else {
            verbose('updating single item...');
            run(dbScanner.updateItem(updateInput.TableName, updateInput));
        }

        break;
    case givenArg('recreate'):
        enforceSafety();
        var snapshot;
        try {
            snapshot = JSON.parse(argv.recreate);
        } catch (e) {
            snapshot = require(argv.recreate);
        }

        say('recreating database...');

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
