'use strict';

var argv = require('./argv');

module.exports = require(argv.config || '../../config');
