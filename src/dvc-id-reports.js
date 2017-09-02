var async = require('async');
var db = require('./db');
var util = require('./util');
var trackers = require('./trackers');

var COLLECTION = 'dvc_id_reports';

module.exports = {
    getTrackerUsageMetrics: getTrackerUsageMetrics,
    getDaysInUseOfTracker: getDaysInUseOfTracker
};

function getTrackerUsageMetrics(startDate, endDate, trackerIds, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryTrackerUsageMetrics, startDate, endDate, trackerIds)
    ], cb)
}


function getDaysInUseOfTracker(data, cb) {
    async.waterfall([
        async.apply(queryDaysInUseOfTracker, data.created_at, data.submit_date, data.tracker_id)
    ], function (err, results) {
        if (err) {
            cb(err, null);
        } else {
            var result;
            if (!!results[0]) {
                result = {_id: results[0]._id.tracker_id, days_in_use: results.length}
            }
            cb(null, result);
        }
    });
}

function queryDaysInUseOfTracker(startDate, endDate, trackerId, cb) {
    var pipeline = [
        {
            '$match': {
                'device_id': trackerId,
                'time': {'$gte': startDate, '$lte': endDate}
            }
        },
        {
            '$project': {'device_id': 1, 'time': 1}
        },
        {
            '$group': {_id: {tracker_id: '$device_id', day: {$dayOfMonth: '$time'}}}
        }
    ];
    db.aggregate(db.getTractiveDbConnection(), COLLECTION, pipeline, cb);
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
    db.aggregate(connection, COLLECTION, pipeline, cb);
}

