/*global describe,it,beforeEach*/
'use strict';
var sinon = require('sinon');
require('chai').should();
var Q = require('q');
var _ = require('lodash');

var DBScanner = require('../lib/db-scanner.js').DBScanner;
var MOCKED_TABLES = ['table1', 'table2'];
var MOCKED_TABLE_DATA = {
    Items: [{
        '_id': '5468bf19be57d72541f6e2c1',
        'index': 0,
        'guid': '83e57512-980f-43c2-94aa-32cd7d844850'
    }, {
        '_id': '5468bf192f0badaa6c682975',
        'index': 1,
        'guid': '1519b5cd-e710-4e91-9e66-9d13ad1b5f3d'
    }, {
        '_id': '5468bf19d223dd9a3d421e07',
        'index': 2,
        'guid': '249da846-5515-41ac-bcfb-7b593c5c363b'
    }, {
        '_id': '5468bf198f9f8ef8f33680d1',
        'index': 3,
        'guid': 'dd164074-5b7f-4200-a190-60ad1c832d81'
    }, {
        '_id': '5468bf19f42a703fbdcddcd4',
        'index': 4,
        'guid': '5e00ac67-8129-4e4b-a293-006668eb3974',
        'a': 'b'
    }]
};

var DIFFERENT_MOCKED_TABLE_DATA = {
    Items: [{
        "email": "rondabanks@brainquil.com",
        "phone": "+1 (968) 595-3724",
        "address": "421 Bragg Court, Belgreen, North Carolina, 1587"
    }, {
        "email": "rondabanks@brainquil.com",
        "phone": "+1 (879) 596-2346",
        "address": "532 Garfield Place, Fidelis, Kansas, 4662"
    }, {
        "email": "rondabanks@brainquil.com",
        "phone": "+1 (978) 495-3874",
        "address": "320 Schweikerts Walk, Tooleville, New Hampshire, 6954"
    }, {
        "email": "rondabanks@brainquil.com",
        "phone": "+1 (949) 504-2296",
        "address": "365 Montieth Street, Rosedale, Washington, 6243"
    }, {
        "email": "rondabanks@brainquil.com",
        "phone": "+1 (923) 514-3837",
        "address": "254 Baltic Street, Winfred, Utah, 9951"
    }]
};

var TABLE_NAME = 'aaa';
var EXPECTED_SCHEMA = Object.keys(MOCKED_TABLE_DATA.Items[0]).concat('a');
// http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#createTable-property
var TABLE_DESCRIPTION = {
    AttributeDefinitions: [ /* required */ {
            AttributeName: 'STRING_VALUE',
            /* required */
            AttributeType: 'S | N | B' /* required */
        },
        /* more items */
    ],
    KeySchema: [ /* required */ {
            AttributeName: 'STRING_VALUE',
            /* required */
            KeyType: 'HASH | RANGE' /* required */
        },
        /* more items */
    ],
    ProvisionedThroughput: { /* required */
        ReadCapacityUnits: 0,
        /* required */
        WriteCapacityUnits: 0 /* required */
    },
    TableName: 'STRING_VALUE',
    /* required */
    GlobalSecondaryIndexes: [{
            IndexName: 'STRING_VALUE',
            /* required */
            KeySchema: [ /* required */ {
                    AttributeName: 'STRING_VALUE',
                    /* required */
                    KeyType: 'HASH | RANGE' /* required */
                },
                /* more items */
            ],
            Projection: { /* required */
                NonKeyAttributes: [
                    'STRING_VALUE',
                    /* more items */
                ],
                ProjectionType: 'ALL | KEYS_ONLY | INCLUDE'
            },
            ProvisionedThroughput: { /* required */
                ReadCapacityUnits: 0,
                /* required */
                WriteCapacityUnits: 0 /* required */
            }
        },
        /* more items */
    ],
    LocalSecondaryIndexes: [{
            IndexName: 'STRING_VALUE',
            /* required */
            KeySchema: [ /* required */ {
                    AttributeName: 'STRING_VALUE',
                    /* required */
                    KeyType: 'HASH | RANGE' /* required */
                },
                /* more items */
            ],
            Projection: { /* required */
                NonKeyAttributes: [
                    'STRING_VALUE',
                    /* more items */
                ],
                ProjectionType: 'ALL | KEYS_ONLY | INCLUDE'
            }
        },
        /* more items */
    ]
};

var TABLE_DESCRIPTION_WITHOUT_SECONDARY_INDEX = {
    AttributeDefinitions: [ /* required */ {
            AttributeName: 'STRING_VALUE',
            /* required */
            AttributeType: 'S | N | B' /* required */
        },
        /* more items */
    ],
    KeySchema: [ /* required */ {
            AttributeName: 'STRING_VALUE',
            /* required */
            KeyType: 'HASH | RANGE' /* required */
        },
        /* more items */
    ],
    ProvisionedThroughput: { /* required */
        ReadCapacityUnits: 0,
        /* required */
        WriteCapacityUnits: 0 /* required */
    },
    TableName: 'STRING_VALUE',
    LocalSecondaryIndexes: [{
            IndexName: 'STRING_VALUE',
            /* required */
            KeySchema: [ /* required */ {
                    AttributeName: 'STRING_VALUE',
                    /* required */
                    KeyType: 'HASH | RANGE' /* required */
                },
                /* more items */
            ],
            Projection: { /* required */
                NonKeyAttributes: [
                    'STRING_VALUE',
                    /* more items */
                ],
                ProjectionType: 'ALL | KEYS_ONLY | INCLUDE'
            }
        },
        /* more items */
    ]
};

function mockedDynamo() {
    return {
        listTables: sinon.stub().callsArgWith(0, null, {
            TableNames: _.cloneDeep(MOCKED_TABLES)
        }),
        scan: sinon.stub().callsArgWith(1, null, _.cloneDeep(MOCKED_TABLE_DATA)),
        describeTable: sinon.stub().callsArgWith(1, null, _.cloneDeep(TABLE_DESCRIPTION)),
        createTable: sinon.stub().callsArgWith(1, null),
        deleteTable: sinon.stub().callsArgWith(1, null),
    };
}

describe('db-scanner node module.', function(done) {
    var dbScanner;

    beforeEach(function() {
        dbScanner = new DBScanner(mockedDynamo());
    });

    it('should list tables in dynamoDB', function(done) {
        dbScanner.listTables().then(function(tables) {
            tables.TableNames.should.eql(MOCKED_TABLES);
            done();
        }).catch(done);
    });

    it('should scan contents of a table', function(done) {
        dbScanner.scanTable(TABLE_NAME).then(function(tableData) {
            tableData.should.eql(MOCKED_TABLE_DATA);
            done();
        }).catch(done);
    });

    it('should scan contents of all tables', function(done) {
        dbScanner.scanAllTables().then(function(tableData) {
            tableData.length.should.equal(MOCKED_TABLES.length);
            tableData.forEach(function(tableData) {
                MOCKED_TABLES.should.contain(tableData.TableName);
                _.omit(tableData, 'TableName').should.eql(MOCKED_TABLE_DATA);
            });

            done();
        }).catch(done);
    });

    it('should get the current schema based on table data', function(done) {
        dbScanner.getTableSchema(TABLE_NAME).then(function(schema) {
            schema.should.eql(EXPECTED_SCHEMA);
            done();
        }).catch(done);
    });

    it('should get a table description', function(done) {
        dbScanner.describeTable(TABLE_NAME).then(function(description) {
            description.should.eql(TABLE_DESCRIPTION);
            done();
        }).catch(done);
    });

    it('should create a table given a correct description', function(done) {
        var dynamo = mockedDynamo();
        dynamo.createTable = function(description, cb) {
            this.scan = sinon.stub().callsArgWith(1, null, _.cloneDeep(DIFFERENT_MOCKED_TABLE_DATA));
            cb(null);
        };

        dbScanner = new DBScanner(dynamo);

        Q.all([
            dbScanner.createTable(TABLE_DESCRIPTION),
            dbScanner.scanTable(TABLE_NAME)
        ]).then(function(data) {
            data[1].should.eql(DIFFERENT_MOCKED_TABLE_DATA);
            done();
        }).catch(done);
    });

    it('should create a table given a correct description without global secondary indexes', function(done) {
        var dynamo = mockedDynamo();
        dynamo.createTable = function(description, cb) {
            this.scan = sinon.stub().callsArgWith(1, null, _.cloneDeep(DIFFERENT_MOCKED_TABLE_DATA));
            cb(null);
        };

        dbScanner = new DBScanner(dynamo);

        Q.all([
            dbScanner.createTable(TABLE_DESCRIPTION_WITHOUT_SECONDARY_INDEX),
            dbScanner.scanTable(TABLE_NAME)
        ]).then(function(data) {
            data[1].should.eql(DIFFERENT_MOCKED_TABLE_DATA);
            done();
        }).catch(done);

    });

    it('should get all available table descriptions', function(done) {
        dbScanner.describeAllTables().then(function(descriptions) {
            descriptions.length.should.equal(MOCKED_TABLES.length);
            descriptions.forEach(function(description) {
                description.should.eql(TABLE_DESCRIPTION);
            });

            done();
        }).catch(done);
    });

    it('should create many tables', function(done) {
        var tablesCreated = 0;
        var dynamo = mockedDynamo();
        dynamo.createTable = function(description, cb) {
            tablesCreated++;
            cb(null);
        };

        dbScanner = new DBScanner(dynamo);

        dbScanner.createManyTables([TABLE_DESCRIPTION, TABLE_DESCRIPTION]).then(function() {
            tablesCreated.should.equal(2);
            done();
        }).catch(done);
    });

    it('should remove a table', function() {
        var tableRemoved = '';
        var dynamo = mockedDynamo();
        dynamo.createTable = function(name, cb) {
            tableRemoved = name;
            cb(null);
        };

        dbScanner = new DBScanner(dynamo);

        dbScanner.deleteTable(TABLE_NAME).then(function() {
            tableRemoved.should.eql(TABLE_NAME);
        }).catch(done);
    });

    it('should remove all tables', function() {
        var tablesRemoved = [];
        var dynamo = mockedDynamo();
        dynamo.createTable = function(name, cb) {
            tablesRemoved.push(name);
            cb(null);
        };

        dbScanner = new DBScanner(dynamo);

        dbScanner.deleteAllTables(TABLE_NAME).then(function() {
            tablesRemoved.should.eql(MOCKED_TABLES);
        }).catch(done);
    });
});
