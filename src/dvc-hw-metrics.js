var async = require('async');
var _ = require('lodash');
var db = require('./db');
var util = require('./util');
var users = require('./users');
var trackers = require('./trackers');

module.exports = {
    getHwStatusMetricsForUsers: getHwStatusMetricsForUsers,
    getErrorLogCount: getErrorLogCount
};

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
    db.aggregate(connection, 'dvc_hw_reports', pipeline, cb);
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
    db.aggregate(connection, 'dvc_hw_reports', pipeline, cb);
}

