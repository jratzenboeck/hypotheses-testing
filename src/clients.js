var async = require('async');
var db = require('./db');
var util = require('./util');

module.exports.getClientIds = getClientIds;

function getClientIds(cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        queryClientIds
    ], function (err, result) {
        cb(err, result);
    });
}

function queryClientIds(connection, cb) {
    return db.find(connection, 'clients', {app_name: 'tractivegps'}, {_id: 1}, cb);
}