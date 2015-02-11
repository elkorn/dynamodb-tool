'use strict';

var argv = require('minimist')(process.argv.slice(2));

module.exports = {
  params: argv,
  given: function givenArg(val) {
    return argv.hasOwnProperty(val);
  }
};
