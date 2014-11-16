/*
 *
 * user/repo
 *
 * Copyright (c) 2014 Korneliusz Caputa
 * Licensed under the MIT license.
 */

'use strict';

var Q = require('q');
var _ = require('lodash');

function handleError(deferred, fn) {
    return function(err) {
        if (err) {
            console.error(err);
            deferred.reject(err);
            return;
        }


        try {
            return fn.apply(this, [].slice.call(arguments, 1));
        } catch (e) {
            deferred.reject(e);
        }
    };
}

function resolve(deferred) {
    return function() {
        deferred.resolve.apply(deferred, arguments);
    };
}

function respondWithData(deferred) {
    return handleError(deferred, resolve(deferred));
}

function def(fn) {
    var deferred = Q.defer();
    fn(deferred);
    return deferred.promise;
}

var args = {
    TableName: function TableName(name) {
        return {
            TableName: name
        };
    }
};


function DBScanner(dynamo) {
    var self = this;

    this.listTables = function() {
        return def(function(deferred) {
            dynamo.listTables(respondWithData(deferred));
        });
    };

    this.scanTable = function(tableName) {
        return def(function(deferred) {
            dynamo.scan(args.TableName(tableName), respondWithData(deferred));
        });
    };

    this.getTableSchema = function(tableName) {
        return def(function(deferred) {
            self.scanTable(tableName).then(function(tableData) {
                try {
                    var schema = Object.keys(tableData.Items.reduce(_.merge));
                    deferred.resolve(schema);
                } catch (e) {
                    deferred.reject(e);
                }
            });
        });
    };

    this.describeTable = function(tableName) {
        return def(function(deferred) {
            dynamo.describeTable(
                args.TableName(tableName),
                respondWithData(deferred));
        });
    };
}

exports.DBScanner = DBScanner;
