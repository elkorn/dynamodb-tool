'use strict';

function DBSnapshot(description, data) {
    this.Description = description;
    this.Data = data;
}

DBSnapshot.create = function(description, data) {
    return new DBSnapshot(description, data);
};

module.exports = DBSnapshot;
