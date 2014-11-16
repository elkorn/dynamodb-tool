#! /usr/bin/env node

'use strict';
var AWS = require('aws-sdk');
AWS.config.region = 'eu-west-1';
var dynamo = new AWS.DynamoDB();
var _ = require('lodash');

var dbScanner = new(require('./lib/db-scanner')).DBScanner(dynamo);
var argv = require('minimist')(process.argv.slice(2));

var wait = false;

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

var finish = stopWaiting(_.compose(log, stringify));

switch (true) {
    case !!argv.scan:
        wait = true;
        dbScanner.scanTable(argv.scan)
            .then(finish)
            .catch(stopWaiting);
        break;
    case !!argv.list:
        dbScanner.listTables()
            .then(finish)
            .catch(stopWaiting);
        break;
    case !!argv.schema:
        dbScanner.getTableSchema(argv.schema)
            .then(finish)
            .catch(stopWaiting);
        break;
    case !!argv.describe:
        dbScanner.describeTable(argv.describe)
            .then(finish)
            .catch(stopWaiting);
        break;
}

doWait();
