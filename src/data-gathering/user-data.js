var async = require('async');
var util = require('../util');
var db = require('../db');

var COLLECTION = 'users';

module.exports = {
    insertUserEmail: insertUserEmail
};

function insertUserEmail(dataInstances, cb) {
    async.waterfall([
        async.apply(getEmailAddresses, dataInstances),
        async.apply(util.mergeData, '_id', 'user_id', dataInstances)
    ], cb);
}

function getEmailAddresses(dataInstances, cb) {
    async.map(dataInstances, getEmail, cb);
}

function getEmail(data, cb) {
    db.findOne(db.getTractiveDbConnection(), COLLECTION, {_id: data.user_id}, {email: 1}, cb);
}