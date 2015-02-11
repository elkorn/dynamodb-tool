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

(function() {
  var args, updateInput, isArray;

  switch (true) {
    case argv.given('scan'):
      return cli.commands.scan;

    case argv.given('list'):
      log.verbose('listing tables...');
      return cli.commands.list;

    case argv.given('schema'):
      log.verbose('getting DB schema...');
      return cli.commands.schema;

    case argv.given('describe'):
      if (typeof(argv.params.describe) === 'string') {
        log.verbose('describing a table...');
        return cli.commands.describe.one;
      } else {
        log.verbose('describing all tables...');
        return cli.commands.describe.all;
      }
      break;

    case argv.given('create'):
      var descriptions = require(argv.params.create);
      if (_.isArray(descriptions)) {
        log.verbose('creating multiple tables...');
        return _.partial(cli.createTable.many, descriptions);
      } else {
        log.verbose('creating a table...');
        return _.partial(cli.createTable.one, descriptions);
      }
      break;

    case argv.given('delete'):
      log.verbose('deleting a table...');
      return cli.commands.delete.one;

    case argv.given('delete-all'):
      log.verbose('deleting all tables...');
      return cli.commands.delete.all;

    case argv.given('snapshot'):
      log.say('creating a snapshot...');
      return cli.commands.snapshot;

    case argv.given('get'):
      log.verbose('getting item...');
      args = parseItemArguments(argv.params.get);
      return _.partial(cli.commands.getItem, args);

    case argv.given('put'):
      args = parseItemArguments(argv.params.put);
      if (_.isArray(args[1])) {
        log.verbose('putting mutliple items...');
        return cli.commands.putItem.many;
      } else {
        log.verbose('putting single item...');
        return cli.commands.putItem.one;
      }
      break;

    case argv.given('update-all'):
      try {
        updateInput = JSON.parse(argv.params['update-all']);
      } catch (e) {
        updateInput = require(argv.params['update-all']);
      }

      isArray = _.isArray(updateInput);

      if (isArray) {
        return cli.commands.update.allIn.manyTables;
      } else {
        log.verbose(require('util').format('updating all items in table %s...', updateInput.TableName));
        return _.partial(cli.commands.update.allIn.oneTable, updateInput);
      }
      break;

    case argv.given('update'):
      try {
        updateInput = JSON.parse(argv.params.update);
      } catch (e) {
        updateInput = require(argv.params.update);
      }

      isArray = _.isArray(updateInput);

      if (isArray) {
        log.verbose('updating multiple items...');
        return cli.commands.update.many;
      } else {
        log.verbose('updating single item...');
        return cli.commands.update.one;
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
      return _.partial(cli.commands.recreate, snapshot);

    default:
      log.say('nothing to do.');
      return cli.commands.noop;
  }

}())(argv, dbScanner);

cli.wait();
