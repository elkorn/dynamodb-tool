#!/usr/bin/env node

'use strict';

var _ = require('lodash');
var argv = require('./lib/util/argv');
var log = require('./lib/util/log');
var modules = require('./lib/main-module-provider');
var cli = require('./lib/util/cli');

function parseItemArguments(argv) {
    if (typeof(argv) !== 'string') {
        throw new Error('The argument for item operations must be a string.');
    }

    var args = argv.paramssplit(/=/);
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
var command;

switch (true) {
    case argv.given('scan'):
        command = cli.commands.scan;
        break;
    case argv.given('list'):
        log.verbose('listing tables...');
        command = cli.commands.list;
        break;
    case argv.given('schema'):
        log.verbose('getting DB schema...');
        command = cli.commands.schema;
        break;
    case argv.given('describe'):
        if (typeof(argv.params.describe) === 'string') {
            log.verbose('describing a table...');
            command = cli.commands.describe.one;
        } else {
            log.verbose('describing all tables...');
            command = cli.commands.describe.all;
        }
        break;
    case argv.given('create'):
        var descriptions = require(argv.params.create);
        if (_.isArray(descriptions)) {
            log.verbose('creating multiple tables...');
            command = _.partial(cli.createTable.many, descriptions);
        } else {
            log.verbose('creating a table...');
            command = _.partial(cli.createTable.one, descriptions);
        }
        break;
    case argv.given('delete'):
        log.verbose('deleting a table...');
        command = cli.commands.delete.one;
        break;
    case argv.given('delete-all'):
        log.verbose('deleting all tables...');
        command = cli.commands.delete.all;
        break;
    case argv.given('snapshot'):
        log.say('creating a snapshot...');
        command = cli.commands.snapshot;
        break;
    case argv.given('get'):
        log.verbose('getting item...');
        var args = parseItemArguments(argv.params.get);
        command = _.partial(cli.commands.getItem, args);
        break;
    case argv.given('put'):
        var args = parseItemArguments(argv.params.put);
        if (_.isArray(args[1])) {
            log.verbose('putting mutliple items...');
            command = cli.commands.putItem.many;
        } else {
            log.verbose('putting single item...');
            command = cli.commands.putItem.one;
        }

        break;
    case argv.given('update-all'):
        var updateInput;
        try {
            updateInput = JSON.parse(argv.params['update-all']);
        } catch (e) {
            updateInput = require(argv.params['update-all']);
        }

        var isArray = _.isArray(updateInput);

        if (isArray) {
            command = cli.commands.update.allIn.manyTables;
        } else {
            log.verbose(require('util').format('updating all items in table %s...', updateInput.TableName));
            command = _.partial(cli.commands.update.allIn.oneTable, updateInput);
        }

        break;

    case argv.given('update'):
        var updateInput;
        try {
            updateInput = JSON.parse(argv.params.update);
        } catch (e) {
            updateInput = require(argv.params.update);
        }

        var isArray = _.isArray(updateInput);

        if (isArray) {
            log.verbose('updating multiple items...');
            command = cli.commands.update.many;
        } else {
            log.verbose('updating single item...');
            command = cli.commands.update.one;
        }

        break;
    case argv.given('recreate'):
        var snapshot;
        try {
            snapshot = JSON.parse(argv.params.recreate);
        } catch (e) {
            snapshot = require(argv.params.recreate);
        }

        log.say('recreating database...');
        command = _.partial(cli.commands.recreate, snapshot);
        break;
    default:
        log.say('nothing to do.');
        command = cli.commands.noop;
}

command(argv,dbScanner);
cli.wait();
