'use strict';

var config = require('./util/config');
var log = require('./util/log');

function makeDynamo() {
  var AWS = require('aws-sdk');
  var result = new AWS.DynamoDB({
    region: config.region || AWS.config.region || 'eu-west-1',
    endpoint: config.endpoint || AWS.config.endpoint
  });

  log.verbose('\nEndpoint: ' + result.endpoint.host + '\n');
  return result;
}

function makeDbScanner() {
  return new(require('./core/db-scanner')).DBScanner(makeDynamo());
}

module.exports = {
  makeDynamo: makeDynamo,
  makeDbScanner: makeDbScanner
};
