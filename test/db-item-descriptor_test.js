/*global describe,it*/
'use strict';
require('chai').should();
var _ = require('lodash');

var itemDescriptor = require('../lib/db-item-descriptor');
var ItemDescriptor = itemDescriptor.ItemDescriptor;
var PutItemDescriptor = itemDescriptor.PutItemDescriptor;
var AwsAttribute = itemDescriptor.AwsAttribute;

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

describe('db-item-descriptor', function() {
    var expected = MOCKED_TABLE_DATA.Items[0];
    it('should be created according to AWS rules', function() {
        var descriptor = new ItemDescriptor(MOCKED_TABLES[0], {
            id: expected._id
        });

        descriptor.should.have.property('Key');
        descriptor.should.have.property('TableName', MOCKED_TABLES[0]);
        var key = descriptor.Key;
        key.should.have.property('id');
        key.id.should.eql({
            S: expected._id
        });
    });

    it('should support many properties', function() {
        var descriptor = new ItemDescriptor(MOCKED_TABLES[0], expected);

        descriptor.should.have.property('Key');
        descriptor.should.have.property('TableName', MOCKED_TABLES[0]);
        var key = descriptor.Key;
        _.forEach(expected, function(value, prop) {
            key.should.have.property(prop);
            key[prop].should.eql(new AwsAttribute(value));
        });
    });

    it('should support PUT request convention', function() {
        var descriptor = new PutItemDescriptor(MOCKED_TABLES[0], expected);
        descriptor.should.have.property('Item');
        descriptor.should.have.property('TableName', MOCKED_TABLES[0]);
        var item = descriptor.Item;
        _.forEach(expected, function(value, prop) {
            item.should.have.property(prop);
            item[prop].should.eql(new AwsAttribute(value));
        });
    });

    it('should parse numeric attributes correctly', function() {
        var descriptor = new ItemDescriptor('test', {
            num: 12
        });

        descriptor.Key.num.N.should.be.a('string');
    });
});
