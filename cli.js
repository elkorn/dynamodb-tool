#! /usr/bin/env node

'use strict';

var _ = require('lodash');
var argv = require('minimist')(process.argv.slice(2));

var config = require('./config');

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

function makeDynamo() {
    var AWS = require('aws-sdk');
    return new AWS.DynamoDB({
        region: config.region || AWS.config.region || 'eu-west-1',
        endpoint: config.endpoint || AWS.config.endpoint
    });
}

function makeDbScanner() {
    return new(require('./lib/db-scanner')).DBScanner(makeDynamo());
}

function verbose(msg) {
    if (config.verbose) {
        console.log(msg);
    }
}

function run(promise) {
    promise
        .then(finish)
        .catch(finish);
}

var dbScanner = makeDbScanner();
verbose('Endpoint: ' + config.endpoint + '\n');

switch (true) {
    case givenArg('scan'):
        run(dbScanner.scanTable(argv.scan));
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
    default:
        wait = false;
}

doWait();
