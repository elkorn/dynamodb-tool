'use strict';

var Q = require('q');
var _ = require('lodash');
var DBSnapshot = require('./db-snapshot');
var itemDescriptor = require('./db-item-descriptor');
var ItemDescriptor = itemDescriptor.ItemDescriptor;
var BatchWriteItemDescriptor = itemDescriptor.BatchWriteItemDescriptor;
var PutItemDescriptor = itemDescriptor.PutItemDescriptor;
var ProgressReporter = require('./progress-reporter.js');

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

function respondWithDataAndProgress(deferred, progressFn) {
    return handleError(deferred, _.compose(progressFn, resolve(deferred)));
}

function def(fn) {
    var deferred = Q.defer();
    fn(deferred);
    return deferred.promise;
}

function getProvisionedThroughput(src) {
    return _.pick(src.ProvisionedThroughput, ['ReadCapacityUnits', 'WriteCapacityUnits']);
}

function getSecondaryIndex(index) {
    return _.pick(index, ['IndexName', 'KeySchema', 'Projection']);
}

function getSecondaryIndexWithProvisionedThroughput(index) {
    var result = getSecondaryIndex(index);
    result.ProvisionedThroughput = getProvisionedThroughput(index);
    return result;
}

function extractDescription(data) {
    var tableData = data.Table || data;
    var result = {
        AttributeDefinitions: tableData.AttributeDefinitions,
        KeySchema: tableData.KeySchema,
        ProvisionedThroughput: getProvisionedThroughput(tableData),
        TableName: tableData.TableName,
    };

    if (tableData.LocalSecondaryIndexes) {
        result.LocalSecondaryIndexes = tableData.LocalSecondaryIndexes.map(getSecondaryIndex);
    }

    if (tableData.GlobalSecondaryIndexes) {
        result.GlobalSecondaryIndexes = tableData.GlobalSecondaryIndexes.map(getSecondaryIndexWithProvisionedThroughput);
    }

    return result;
}

var args = {
    TableName: function TableName(name) {
        return {
            TableName: name
        };
    }
};

var DYNAMO_BATCH_SIZE = 25;

function inBatches(data, batchSize) {
    return {
        execute: function(fn) {
            var result = [];
            if (data.length <= batchSize) {
                result.push(fn(data));
            } else {
                var i = 1;
                for (var len = Math.ceil(data.length / batchSize); i <= len; i++) {
                    result.push(fn(data.slice((i - 1) * batchSize, i * batchSize)));
                }
            }

            return result;
        }
    };
}

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

    this.scanAllTables = function() {
        return this.listTables().then(function(response) {
            return Q.all(response.TableNames.map(function(tableName) {
                return self.scanTable(tableName).then(function(tableData) {
                    tableData.TableName = tableName;
                    return tableData;
                });
            }));
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

    this.describeAllTables = function() {
        return this.listTables().then(function(response) {
            return Q.all(response.TableNames.map(self.describeTable));
        });
    };

    this.createTable = function(tableDescription) {
        return def(function(deferred) {
            dynamo.createTable(extractDescription(tableDescription), respondWithData(deferred));
        });
    };

    this.createManyTables = function(tableDescriptions) {
        return Q.all(tableDescriptions.map(self.createTable));
    };

    this.deleteTable = function(tableName) {
        return def(function(deferred) {
            dynamo.deleteTable(args.TableName(tableName), respondWithData(deferred));
        });
    };

    this.deleteAllTables = function() {
        return this.listTables().then(function(response) {
            return Q.all(response.TableNames.map(self.deleteTable));
        });
    };

    this.createSnapshot = function() {
        var progressReporter = new ProgressReporter();

        return this.listTables().then(function(response) {
            return Q.all(response.TableNames.map(function(tableName) {
                progressReporter.addTarget(1);
                return Q.spread([
                            self.describeTable(tableName),
                            self.scanTable(tableName)
                        ],
                        DBSnapshot.create)
                    .then(function(snapshot) {
                        progressReporter.addProgress(1);
                        return snapshot;
                    });

            }));
        });
    };

    this.getItem = function(tableName, item) {
        return def(function(deferred) {
            dynamo.getItem(
                new ItemDescriptor(tableName, item),
                respondWithData(deferred));
        });
    };

    this.putItem = function(tableName, item) {
        return def(function(deferred) {
            dynamo.putItem(
                new PutItemDescriptor(tableName, item),
                respondWithData(deferred));
        });
    };

    this.putMultipleItems = function(tableName, items, progressReporter) {
        return Q.all(inBatches(items, DYNAMO_BATCH_SIZE)
            .execute(function(items) {
                return def(function(deferred) {
                    var responseHandler;
                    if (progressReporter) {
                        responseHandler = respondWithDataAndProgress(deferred, function() {
                            progressReporter.addProgress(items.length);
                        });
                    } else {
                        responseHandler = respondWithData(deferred);
                    }

                    dynamo.batchWriteItem(
                        new BatchWriteItemDescriptor(tableName, items),
                        responseHandler);
                });
            }));
    };

    this.recreateFromSnapshot = function(snapshot) {
        var progressReporter = new ProgressReporter();
        return Q.all(snapshot.map(function(snapshot) {
            progressReporter.addTarget(snapshot.Data.Items.length);
            return self.createTable(snapshot.Description.Table)
                .then(
                    _.partial(
                        self.putMultipleItems,
                        snapshot.Description.Table.TableName,
                        snapshot.Data.Items,
                        progressReporter));
        }));
    };
}

exports.DBScanner = DBScanner;
