'use strict';

var ProgressBar = require('progress');

var DEFAULT_TEMPLATE = '[:bar] :percent :etas';

function ProgressReporter(options) {
    var target = 0;
    var bar;

    function tick(val) {
        if (!bar) {
            bar = new ProgressBar(options.template || DEFAULT_TEMPLATE, {
                complete: '=',
                incomplete: ' ',
                width: 30,
                total: target
            });
        }

        bar.tick(val);
    }

    options = options || {};

    this.addTarget = function(val) {
        target += val;
    };

    this.addProgress = function(val) {
        tick(val);
    };
}

module.exports = ProgressReporter;
