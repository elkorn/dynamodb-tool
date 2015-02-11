/*global describe,it*/
'use strict';
require('chai').should();
var _ = require('lodash');

var itemDescriptor = require('../lib/core/db-item-descriptor');
var ItemDescriptor = itemDescriptor.ItemDescriptor;
var ItemValueDescriptor = itemDescriptor.ItemValueDescriptor;
var PutItemDescriptor = itemDescriptor.PutItemDescriptor;
var BatchWriteItemDescriptor = itemDescriptor.BatchWriteItemDescriptor;
var UpdateItemDescriptor = itemDescriptor.UpdateItemDescriptor;
var KeyItemDescriptor = itemDescriptor.KeyItemDescriptor;
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

    it('should recognize AWS attributes', function() {
        var attrs = [{
            N: 'aaa'
        }, {
            S: 'aaa'
        }, {
            M: {
                x: {
                    M: {
                        y: {
                            S: 'aaa'
                        }
                    }
                }
            }
        }, {
            L: ['aaa']
        }, {
            SS: ['aaa']
        }, {
            NULL: null
        }, {
            BOOL: false
        }];

        attrs.forEach(function(attr) {
            var awsAttr = new AwsAttribute(attr);
            awsAttr.should.eql(attr);
        });
    });

    it('should support batch write request', function() {
        var data = [{
            num: 12
        }, {
            num: 13
        }];
        var descriptor = new BatchWriteItemDescriptor('test', data);

        descriptor.should.have.property('RequestItems');
        var items = descriptor.RequestItems;
        items.should.have.property('test');
        items.test.should.eql(data.map(function(item) {
            return {
                PutRequest: {
                    Item: new ItemValueDescriptor(item)
                }
            };
        }));
    });

    it('should create correct item descriptors from nested objects', function() {
        var obj = {
            x: {
                y: 12
            }
        };

        var expected = {
            x: {
                M: {
                    y: {
                        N: "12"
                    }
                }
            }
        };

        var descriptor = new ItemDescriptor(MOCKED_TABLES[0], obj);
        descriptor.Key.x.should.eql(expected.x);
    });

    it('should create a correct descriptor for udpating an existing entry', function() {
        var tableName = 'test';
        var obj = {
            Key: {
                testName: 'testValue'
            },
            Put: {
                testPut: 1
            },
            Add: {
                testAdd: 2
            },
            Delete: {
                testDelete: 3
            }
        };

        var result = new UpdateItemDescriptor(tableName, obj);
        result.TableName.should.eql(tableName);
        result.AttributeUpdates.testPut.should.eql({
            Action: 'PUT',
            Value: {
                N: obj.Put.testPut.toString()
            }
        });
        result.AttributeUpdates.testAdd.should.eql({
            Action: 'ADD',
            Value: {
                N: obj.Add.testAdd.toString()
            }
        });
        result.AttributeUpdates.testDelete.should.eql({
            Action: 'DELETE',
            Value: {
                N: obj.Delete.testDelete.toString()
            }
        });
    });

    it('should leave a value descriptor alone if it already is AWS-compliant', function() {
        var obj = {
            Key: {
                testName: 'testValue'
            },
            Put: {
                testPut: 1
            },
            Add: {
                testAdd: 2
            },
            Delete: {
                testDelete: 3
            }
        };

        (new ItemValueDescriptor(new ItemValueDescriptor(obj))).should.eql(new ItemValueDescriptor(obj));

    });

    it('should extract key values from an item', function() {
        var tableName = 'test';
        var item = {
            testKey1: 'a',
            testKey2: 'b',
            testVal: 13
        };

        var tableDesc = {
            KeySchema: [{
                AttributeName: 'testKey1'
            }, {
                AttributeName: 'testKey2'
            }]
        };

        var descriptor = new KeyItemDescriptor(tableName, tableDesc, item);
        descriptor.should.have.property('TableName', tableName);
        descriptor.should.have.property('Key');
        var key = descriptor.Key;
        key.should.have.property('testKey1', new AwsAttribute(_.pick(item, 'testKey1')).testKey1);
        key.should.have.property('testKey2', new AwsAttribute(_.pick(item, 'testKey2')).testKey1);
    });
});
