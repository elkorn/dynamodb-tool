'use strict';

var _ = require('lodash');
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

var AwsAttribute;

function ItemValueDescriptor(keyObject) {
  var isAwsCompatible = _.every(keyObject, isAwsAttribute);
  if (isAwsCompatible) {
    return keyObject;
  }

  return _.reduce(keyObject, function(result, value, key) {
    result[key] = new AwsAttribute(value);
    return result;
  }, {});
}

AwsAttribute = function(val) {
  var result;
  if (isAwsAttribute(val)) {
    return val;
  } else if (typeof(val) === 'object') {
    result = new ItemValueDescriptor(val);
  } else {
    result = val.toString();
  }

  this[getAwsType(val)] = result;
};

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

function UpdateItemDescriptor(tableName, keyObject) {
  var result = {
    TableName: tableName,
    Key: new ItemValueDescriptor(keyObject.Key),
    AttributeUpdates: {}
  };

  ['Put', 'Add', 'Delete'].forEach(function(operation) {
    _.forEach(new ItemValueDescriptor(keyObject[operation]), function(val, attr) {
      result.AttributeUpdates[attr] = {
        Action: operation.toUpperCase(),
        Value: val
      };
    });
  });

  return result;
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

function KeyItemDescriptor(tableName, tableDescription, keyObject) {
  return {
    Key: tableDescription.KeySchema.reduce(function(result, item) {
      result[item.AttributeName] = keyObject[item.AttributeName];
      return result;
    }, {}),
    TableName: tableName
  };
}

exports.BatchWriteItemDescriptor = BatchWriteItemDescriptor;
exports.ItemDescriptor = ItemDescriptor;
exports.ItemValueDescriptor = ItemValueDescriptor;
exports.PutItemDescriptor = PutItemDescriptor;
exports.UpdateItemDescriptor = UpdateItemDescriptor;
exports.KeyItemDescriptor = KeyItemDescriptor;
exports.AwsAttribute = AwsAttribute;
