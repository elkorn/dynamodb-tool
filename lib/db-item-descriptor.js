'use strict';
var _ = require('lodash');
/*
{
    Key: {
        someKey: {
            B: new Buffer('...') || 'STRING_VALUE',
            BOOL: true || false,
            BS: [
                new Buffer('...') || 'STRING_VALUE',
            ],
            L: [],
            M: {
            },
            N: 'STRING_VALUE',
            NS: [
                'STRING_VALUE',
            ],
            NULL: true || false,
            S: 'STRING_VALUE',
            SS: [
                'STRING_VALUE',
            ]
        },
    },
    TableName: 'STRING_VALUE',
    AttributesToGet: [
        'STRING_VALUE',
    ],
    ConsistentRead: true || false,
    ExpressionAttributeNames: {
        someKey: 'STRING_VALUE',
    },
    ProjectionExpression: 'STRING_VALUE',
    ReturnConsumedCapacity: 'INDEXES | TOTAL | NONE'
};
 */
/*
S — (String)
A String data type.

N — (String)
A Number data type.

B — (Buffer, Typed Array, Blob, String)
A Binary data type.

SS — (Array<String>)
A String Set data type.

NS — (Array<String>)
A Number Set data type.

BS — (Array<Buffer, Typed Array, Blob, String>)
A Binary Set data type.

M — (map<map>)
A Map of attribute values.

L — (Array<map>)
A List of attribute values.

NULL — (Boolean)
A Null data type.

BOOL — (Boolean)
A Boolean data type.
*/
var TYPES = {
    number: 'N',
    string: 'S',
    object: 'M',
    array: 'L',
    stringArray: 'SS',
    numberArray: 'NS',
    'null': 'NULL',
    boolean: 'BOOL'
};

function getAwsType(item) {
    if (item === null) {
        return TYPES['null'];
    }

    if (_.isArray(item)) {
        if (item.every(function(item) {
            return typeof(item) === 'string';
        })) {
            return TYPES.stringArray;
        } else if (item.every(function(item) {
            return !isNaN(item);
        })) {
            return TYPES.numberArray;
        } else {
            return TYPES.array;
        }
    }

    return TYPES[typeof(item)];
}

function isAwsAttribute(val) {
    if (!_.isObject(val)) {
        return false;
    }

    return _.some(TYPES, function(typeVal) {
        return val.hasOwnProperty(typeVal);
    });
}

function AwsAttribute(val) {
    if (isAwsAttribute(val)) {
        return val;
    } else {
        this[getAwsType(val)] = val.toString();
    }
}

function ItemValueDescriptor(keyObject) {
    return _.reduce(keyObject, function(result, value, key) {
        result[key] = new AwsAttribute(value);
        return result;
    }, {});
}

function ItemDescriptor(tableName, keyObject) {
    var result = {
        Key: new ItemValueDescriptor(keyObject),
        TableName: tableName
    };


    return result;
}

function PutItemDescriptor(tableName, keyObject) {
    return {
        Item: new ItemValueDescriptor(keyObject),
        TableName: tableName
    };
}

function BatchWriteItemDescriptor(tableName, keyObjects) {
    var result = {
        RequestItems: {}
    };

    result.RequestItems[tableName] = keyObjects.map(function(item) {
        return {
            PutRequest: {
                Item: new ItemValueDescriptor(item)
            }
        };
    });
    return result;
}

exports.BatchWriteItemDescriptor = BatchWriteItemDescriptor;
exports.ItemDescriptor = ItemDescriptor;
exports.ItemValueDescriptor = ItemValueDescriptor;
exports.PutItemDescriptor = PutItemDescriptor;
exports.AwsAttribute = AwsAttribute;
