#! /usr/bin/env node

'use strict';

var _ = require('lodash');
var argv = require('minimist')(process.argv.slice(2));

var config = require('./config');

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

function given(val) {
    return !!val;
}

function enforceSafety() {
    if (!(given(argv.unsafe) || given(argv.u) || /http[s]?:\/\/0\.0\.0\.0(:\d{4})*/.test(config.endpoint))) {
        throw new Error("Connect to local DynamoDB instance or enable unsafe mode and face the consequences.");
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
    case given(argv.scan):
        wait = true;
        dbScanner.scanTable(argv.scan)
            .then(finish)
            .catch(stopWaiting);
        break;
    case given(argv.list):
        dbScanner.listTables()
            .then(finish)
            .catch(stopWaiting);
        break;
    case given(argv.schema):
        dbScanner.getTableSchema(argv.schema)
            .then(finish)
            .catch(stopWaiting);
        break;
    case given(argv.describe):
        dbScanner.describeTable(argv.describe)
            .then(finish)
            .catch(stopWaiting);
        break;
    case given(argv.create):
        enforceSafety();
        dbScanner.createTable(require(argv.create))
            .then(finish)
            .catch(stopWaiting);
        break;
}

doWait();
