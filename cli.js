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
    if (!(givenArg('unsafe') || givenArg('u') || /http[s]?:\/\/0\.0\.0\.0(:\d{4})*/.test(config.endpoint))) {
        throw new Error("Connect to local DynamoDB instance or enable --unsafe mode and face the consequences.");
    }
}

var finish = stopWaiting(_.compose(log, stringify));

function verbose(msg) {
    if (config.verbose) {
        console.log(msg);
    }
}

function makeDynamo() {
    var AWS = require('aws-sdk');
    var result = new AWS.DynamoDB({
        region: config.region || AWS.config.region || 'eu-west-1',
        endpoint: config.endpoint || AWS.config.endpoint
    });

    verbose('Endpoint: ' + result.endpoint.host + '\n');
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
    try {
        args[1] = JSON.parse(args[1]);
    } catch (e) {
        if (typeof(args[1]) !== 'string') {
            throw new Error('Invalid item descriptor provided.');
        }

        args[1] = require(args[1]);
    }

    console.log(args);
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
        run(dbScanner.listTables());
        break;
    case givenArg('schema'):
        run(dbScanner.getTableSchema(argv.schema));
        break;
    case givenArg('describe'):
        if (typeof(argv.describe) === 'string') {
            run(dbScanner.describeTable(argv.describe));
        } else {
            run(dbScanner.describeAllTables());
        }
        break;
    case givenArg('create'):
        enforceSafety();
        var descriptions = require(argv.create);
        if (_.isArray(descriptions)) {
            run(dbScanner.createManyTables(descriptions));
        } else {
            run(dbScanner.createTable(descriptions));
        }
        break;
    case givenArg('delete'):
        enforceSafety();
        run(dbScanner.deleteTable(argv.delete));
        break;
    case givenArg('delete-all'):
        enforceSafety();
        run(dbScanner.deleteAllTables());
        break;
    case givenArg('snapshot'):
        run(dbScanner.createSnapshot());
        break;
    case givenArg('get'):
        var args = parseItemArguments(argv.get);
        run(dbScanner.getItem(args[0], args[1]));
        break;
    case givenArg('put'):
        enforceSafety();
        var args = parseItemArguments(argv.put);
        if (_.isArray(args[1])) {
            run(dbScanner.putMultipleItems(args[0], args[1]));
        } else {
            run(dbScanner.putItem(args[0], args[1]));
        }

        break;
    case givenArg('recreate'):
        var snapshot;
        try {
            snapshot = JSON.parse(argv.recreate);
        } catch (e) {
            snapshot = require(argv.recreate);
        }

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
