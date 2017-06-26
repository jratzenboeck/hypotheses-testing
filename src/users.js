var async = require('async');
var db = require('./db');

module.exports = {
    getRandomUserIds: getRandomUserIds,
    getUserIdsForTrackerIds: getUserIdsForTrackerIds
};

function getRandomUserIds(createdBefore, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryRandomUserIds, createdBefore)
    ], function (err, result) {
        cb(err, result);
    });
}

function queryRandomUserIds(createdBefore, connection, cb) {
    var pipeline = [
        {'$match': {status: 'ACTIVE', created_at: {$lte: createdBefore}}},
        {'$group': {_id: '$user_id', countTrackers: {$sum: 1}}},
        {'$match': {countTrackers: 1}},
        {'$skip': Math.random() * 100},
        {'$limit': 5000}
    ];
    db.aggregate(connection, 'ppl_subscriptions', pipeline, cb);
}

function getUserIdsForTrackerIds(trackerIds, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryUserIdsForTrackerIds, trackerIds)
    ], function (err, result) {
        cb(err, result);
    });
}

function queryUserIdsForTrackerIds(trackerIds, connection, cb) {
    var pipeline = [
        {'$match': {status: 'ACTIVE', tracker_id: {$in: trackerIds}}},
        {'$group': {'_id': '$user_id'}}
    ];
    return db.aggregate(connection, 'ppl_subscriptions', pipeline, cb);
}