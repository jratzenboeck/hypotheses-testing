var async = require('async');
var db = require('./db');
var util = require('./util');
var users = require('./users');

module.exports = {
    getRandomTrackerIds: getRandomTrackerIds,
    getDeviceData: getDeviceData
};

function getDeviceData(data, cb) {
    findDeviceData(data.tracker_id, cb);
}

function findDeviceData(trackerId, cb) {
    return db.findOne(db.getTractiveDbConnection(), 'devices',
        {
            _id: trackerId
        },
        {model_number: 1, sim_type: 1, hw_edition: 1, fw_version: 1}, cb);
}

function getTrackerIdsOfUsers(userIds, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(findTrackersOfUsers, userIds)
    ], function(err, result) {
        cb(err, result);
    });
}

function getRandomTrackerIds(createdBefore, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryRandomTrackers, createdBefore)
    ], cb);
}

function queryRandomTrackers(createdBefore, connection, cb) {
    var pipeline = [
        {'$match': {status: 'ACTIVE', created_at: {$lte: createdBefore}}},
        {'$skip': Math.random() * 100},
        {'$limit': 1000}
    ];
    db.aggregate(connection, 'ppl_subscriptions', pipeline, cb);
}

function findTrackersOfUsers(userIds, connection, cb) {
    return db.find(connection, 'ppl_subscriptions', {'user_id': {$in: util.getObjectIdsAsStringArray(userIds, '_id')}}, {'tracker_id': 1}, cb);
}

function getDamagesForRandomDevices(startDate, endDate, cb) {
    async.waterfall([
        async.apply(users.getRandomSubscriptions, startDate),
        async.apply(findDamagesForRandomDevices, startDate, endDate)
    ], cb);
}

function findDamagesForRandomDevices(startDate, endDate, subscriptions, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryDamagesForRandomDevices, startDate, endDate,
                    util.getObjectIdsAsStringArray(subscriptions, '_id.tracker_id'))
    ], cb);
}

