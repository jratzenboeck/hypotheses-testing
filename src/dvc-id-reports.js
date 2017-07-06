var async = require('async');
var db = require('./db');
var util = require('./util');
var trackers = require('./trackers');

module.exports = {
    getTrackerUsageMetrics: getTrackerUsageMetrics
};

function getTrackerUsageMetrics(startDate, endDate, trackerIds, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryTrackerUsageMetrics, startDate, endDate, trackerIds)
    ], cb)
}

function queryTrackerUsageMetrics(startDate, endDate, trackerIds, connection, cb) {
    var pipeline = [
        {
            '$match': {
                'device_id': {$in: trackerIds},
                'time': {$gte: startDate, $lt: endDate}
            }
        },
        {
            '$group': {'_id': '$device_id', trackerUsage: {$sum: 1}}
        }
    ];
    db.aggregate(connection, 'dvc_id_reports', pipeline, cb);
}

