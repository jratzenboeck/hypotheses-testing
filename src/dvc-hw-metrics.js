var async = require('async');
var _ = require('lodash');
var db = require('./db');
var util = require('./util');
var users = require('./users');
var trackers = require('./trackers');

var COLLECTION = 'dvc_hw_reports';

module.exports = {
    getHwStatusMetricsForUsers: getHwStatusMetricsForUsers,
    getErrorLogCount: getErrorLogCount,
    getAverageBatteryLifeTimeForTrackers: getAverageBatteryLifeTimeForTrackers
};

function getAverageBatteryLifeTimeForTrackers(startDate, endDate, cb) {
    async.waterfall([
        async.apply(users.getRandomSubscriptions, startDate),
        async.apply(findAverageBatteryLifeTimeForTrackers, startDate, endDate)
    ], cb);
}

function findAverageBatteryLifeTimeForTrackers(startDate, endDate, subscriptions, cb) {
    async.waterfall([
       async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryBatteryLevelsForTrackers, startDate, endDate, util.getObjectIdsAsStringArray(subscriptions, 'tracker_id')),
        filterBatteryLevelsForUsers
    ], cb);
}

function queryBatteryLevelsForTrackers(startDate, endDate, trackerIds, connection, cb) {
    var pipeline = [
        {
            '$match': {
                'device_id': {$in: trackerIds},
                'time': {$gte: startDate, $lte: endDate}
            }
        },
        {
            '$sort': {'time': 1}
        },
        {
            '$group': {
                '_id': '$device_id',
                'hwReportDetails': {$push: {batteryLevel: '$battery_level', time: '$time', hwStatus: '$hw_status'}}
            }
        }
    ];
    db.aggregate(connection, COLLECTION, pipeline, cb);
}

function filterBatteryLevelsForUsers(hwReports, cb) {
    var results = [];
    var timeDifferences = [];
    var timeAtMaxBattery;

    _.forEach(hwReports, function(hwReport) {
        _.forEach(hwReport.hwReportDetails, function(reportDetails) {
            if (!timeAtMaxBattery && hwStatusAllowed(reportDetails.hwStatus) && reportDetails.batteryLevel === 100) {
                timeAtMaxBattery = reportDetails.time;
            } else if (!!timeAtMaxBattery && reportDetails.batteryLevel === 0) {
                var timeDiff = Math.abs(reportDetails.time.getTime() - timeAtMaxBattery.getTime());
                timeDifferences.push(timeDiff);
                timeAtMaxBattery = null;
            } else if (!!timeAtMaxBattery && !hwStatusAllowed(reportDetails.hwStatus)) {
                timeAtMaxBattery = null;
            } else {
                // Nothing to do
            }
        });
        if (timeDifferences.length) {
            results.push({_id: {tracker_id: hwReport._id}, batteryLifeTime: _.mean(timeDifferences)});
            timeDifferences = [];
        }
    });
    cb(null, results);
}

function hwStatusAllowed(hwStatus) {
    return !hwStatus || (hwStatus !== 'BATT_CHARGING' && !hwStatus.match('SYSTEM' + /\w+/));
}

function getHwStatusMetricsForUsers(hwStatus, startDate, endDate, sampleSize, cb) {
    async.waterfall([
        async.apply(trackers.getRandomTrackerIds, startDate),
        async.apply(findHwStatusMetricsForTrackers, hwStatus, startDate, endDate, sampleSize)
    ], cb);
}

function findHwStatusMetricsForTrackers(hwStatus, startDate, endDate, sampleSize, trackerIds, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryHwStatusMetricsForUsers, hwStatus,
            startDate, endDate, sampleSize, util.getObjectIdsAsStringArray(trackerIds, 'tracker_id'))
    ], cb);
}

function queryHwStatusMetricsForUsers(hwStatus, startDate, endDate, sampleSize, trackerIds, connection, cb) {
    var hwStatusAvgFieldName = _.camelCase(hwStatus + '_average');
    var matchCriteria = {
        'hw_status': {$in: hwStatus},
        'time': {
            $gte: startDate,
            $lte: endDate
        },
        'device_id': {$in: trackerIds}
    };
    var groupCriteria = {};
    groupCriteria['_id'] = '$device_id';
    groupCriteria[hwStatusAvgFieldName] = {$sum: 1};

    var projectCriteria = {};
    projectCriteria['_id'] = {$arrayElemAt: ['$users.user_id', 0]};
    projectCriteria[hwStatusAvgFieldName] = 1;

    var pipeline = [
        {'$match': matchCriteria},
        {'$group': groupCriteria},
        {
            '$lookup': {
                from: 'ppl_subscriptions',
                localField: '_id',
                foreignField: 'tracker_id',
                as: 'users'
            }
        },
        {'$project': projectCriteria},
        {'$limit': sampleSize}
    ];
    db.aggregate(connection, COLLECTION, pipeline, cb);
}

function getErrorLogCount(startDate, endDate, sampleSize, cb) {
    async.waterfall([
        async.apply(trackers.getRandomTrackerIds, startDate),
        async.apply(findErrorLogCountForTrackers, startDate, endDate, sampleSize)
    ], cb);
}

function findErrorLogCountForTrackers(startDate, endDate, sampleSize, trackerIds, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryErrorLogCount,
            startDate, endDate, sampleSize, util.getObjectIdsAsStringArray(trackerIds, 'tracker_id'))
    ], cb);
}

function queryErrorLogCount(startDate, endDate, sampleSize, trackerIds, connection, cb) {
    var matchCriteria = {
        'time': {
            $gte: startDate,
            $lte: endDate
        },
        'device_id': {$in: trackerIds}
    };
    var groupCriteria = {};
    groupCriteria['_id'] = '$device_id';
    groupCriteria['sum_err_log_count'] = {$sum: '$err_log_count'};

    var projectCriteria = {};
    projectCriteria['_id'] = {$arrayElemAt: ['$users.user_id', 0]};
    projectCriteria['sum_err_log_count'] = 1;

    var pipeline = [
        {'$match': matchCriteria},
        {'$group': groupCriteria},
        {
            '$lookup': {
                from: 'ppl_subscriptions',
                localField: '_id',
                foreignField: 'tracker_id',
                as: 'users'
            }
        },
        {'$project': projectCriteria},
        {'$limit': sampleSize}
    ];
    db.aggregate(connection, COLLECTION, pipeline, cb);
}

