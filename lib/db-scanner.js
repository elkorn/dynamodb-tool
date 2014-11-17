'use strict';

var Q = require('q');
var _ = require('lodash');
var DBSnapshot = require('./db-snapshot');
var itemDescriptor = require('./db-item-descriptor');
var ItemDescriptor = itemDescriptor.ItemDescriptor;
var BatchWriteItemDescriptor = itemDescriptor.BatchWriteItemDescriptor;
var PutItemDescriptor = itemDescriptor.PutItemDescriptor;

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

function getProvisionedThroughput(src) {
    return _.pick(src.ProvisionedThroughput, 'ReadCapacityUnits', 'WriteCapacityUnits');
}

function getSecondaryIndex(index) {
    var result = _.pick(index, 'IndexName', 'KeySchema', 'Projection', 'ProvisionedThroughput');
    result.ProvisionedThroughput = getProvisionedThroughput(result);
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
        result.GlobalSecondaryIndexes = tableData.GlobalSecondaryIndexes.map(getSecondaryIndex);
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
        return this.listTables().then(function(response) {
            return Q.all(response.TableNames.map(function(tableName) {
                return Q.spread([
                        self.describeTable(tableName),
                        self.scanTable(tableName)
                    ],
                    DBSnapshot.create);
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

    this.putMultipleItems = function(tableName, items) {
        return Q.all(inBatches(items, DYNAMO_BATCH_SIZE)
            .execute(function(items) {
                return def(function(deferred) {
                    dynamo.batchWriteItem(
                        new BatchWriteItemDescriptor(tableName, items),
                        respondWithData(deferred));
                });
            }));
    };
}

exports.DBScanner = DBScanner;
