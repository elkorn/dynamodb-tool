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

var dbScanner = makeDbScanner();

switch (true) {
    case givenArg('scan'):
        dbScanner.scanTable(argv.scan)
            .then(finish)
            .catch(finish);
        break;
    case givenArg('list'):
        dbScanner.listTables()
            .then(finish)
            .catch(finish);
        break;
    case givenArg('schema'):
        dbScanner.getTableSchema(argv.schema)
            .then(finish)
            .catch(finish);
        break;
    case givenArg('describe'):
        if (typeof(argv.describe) === 'string') {
            dbScanner.describeTable(argv.describe)
                .then(finish)
                .catch(finish);
        } else {
            console.log('describing all tables');
            dbScanner.describeAllTables()
                .then(finish)
                .catch(finish);
        }
        break;
    case givenArg('create'):
        enforceSafety();
        var descriptions = require(argv.create);
        if (_.isArray(descriptions)) {
            dbScanner.createManyTables(descriptions)
                .then(finish)
                .catch(finish);
        } else {
            dbScanner.createTable(descriptions)
                .then(finish)
                .catch(finish);
        }
        break;
    default:
        wait = false;
}

doWait();
