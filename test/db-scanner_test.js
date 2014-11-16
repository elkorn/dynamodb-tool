/*global describe,it*/
'use strict';
var sinon = require('sinon');
// var rewire = require('rewire');
// var assert = require('assert');
require('chai').should();

var DBScanner = require('../lib/db-scanner.js').DBScanner;
var MOCKED_TABLES = ['table1', 'table2'];
var MOCKED_TABLE_DATA = [{
    "_id": "5468bf19be57d72541f6e2c1",
    "index": 0,
    "guid": "83e57512-980f-43c2-94aa-32cd7d844850"
}, {
    "_id": "5468bf192f0badaa6c682975",
    "index": 1,
    "guid": "1519b5cd-e710-4e91-9e66-9d13ad1b5f3d"
}, {
    "_id": "5468bf19d223dd9a3d421e07",
    "index": 2,
    "guid": "249da846-5515-41ac-bcfb-7b593c5c363b"
}, {
    "_id": "5468bf198f9f8ef8f33680d1",
    "index": 3,
    "guid": "dd164074-5b7f-4200-a190-60ad1c832d81"
}, {
    "_id": "5468bf19f42a703fbdcddcd4",
    "index": 4,
    "guid": "5e00ac67-8129-4e4b-a293-006668eb3974"
}];

var TABLE_NAME = 'aaa';
var EXPECTED_SCHEMA = Object.keys(MOCKED_TABLE_DATA[0]);
var TABLE_DESCRIPTION = {};

function mockedDynamo() {
    return {
        listTables: sinon.stub().callsArgWith(0, null, MOCKED_TABLES),
        scan: sinon.stub().callsArgWith(1, null, MOCKED_TABLE_DATA),
        describeTable: sinon.stub().callsArgWith(1, null, TABLE_DESCRIPTION),
   };
}

describe('db-scanner node module.', function() {
    var dbScanner = new DBScanner(mockedDynamo());
    it('should list tables in dynamoDB', function(done) {
        dbScanner.listTables().then(function(tables) {
            tables.should.eql(MOCKED_TABLES);
            done();
        });
    });

    it('should scan contents of a table', function(done) {
        dbScanner.scanTable(TABLE_NAME).then(function(tableData) {
            tableData.should.eql(MOCKED_TABLE_DATA);
            done();
        });
    });

    it('should get the current schema based on table data', function(done) {
        dbScanner.getTableSchema(TABLE_NAME).then(function(schema) {
            schema.should.eql(EXPECTED_SCHEMA);
            done();
        });
    });

    it('should get a table description', function(done) {
        dbScanner.describeTable(TABLE_NAME).then(function(description) {
            description.should.eql(TABLE_DESCRIPTION);
            done();
        });
    });
});
