'use strict';

var config = require('./config');

function say(msg) {
  process.stderr.write(msg + '\n');
}

function verbose(msg) {
  if (config.verbose) {
    say(msg);
  }
}

exports.say = say;
exports.verbose = verbose;
