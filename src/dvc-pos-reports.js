var async = require('async');
var _ = require('lodash');
var db = require('./db');
var util = require('./util');
var users = require('./users');

var COLLECTION = 'dvc_pos_reports';

module.exports = {
    getAveragePosReportStatisticValueForRandomTrackers: getAveragePosReportStatisticValueForRandomTrackers,
    getAveragePosReportStatisticForTracker: getAveragePosReportStatisticForTracker,
    getNumberOfSensorReports: getNumberOfSensorReports
};

function getAveragePosReportStatisticForTracker(posReportField, allowedSensors, data, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryAveragePosReportStatistic, posReportField, allowedSensors, data.created_at, data.submit_date, data.tracker_id)
    ], function (err, results) {
        cb(err, results[0]);
    });
}

function getNumberOfSensorReports(sensorUsed, data, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryNumberOfSensorReports, sensorUsed, data.created_at, data.submit_date, data.tracker_id)
    ], function (err, results) {
        cb(err, results[0]);
    });
}

function getAveragePosReportStatisticValueForRandomTrackers(posReportField, startDate, endDate, allowedSensors, sampleSize, cb) {
    async.waterfall([
        async.apply(users.getRandomSubscriptions, startDate),
        async.apply(findAveragePosReportStatistValueForTrackers, posReportField, startDate, endDate, allowedSensors, sampleSize)
    ], cb);
}

function findAveragePosReportStatistValueForTrackers(posReportField, startDate, endDate, allowedSensors, sampleSize, subscriptions, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(queryAveragePosReportStatisticValueForTrackers,
            posReportField, startDate, endDate,
            allowedSensors, sampleSize, util.getObjectIdsAsStringArray(subscriptions, 'tracker_id'))
    ], cb);
}

function queryAveragePosReportStatisticValueForTrackers(posReportField, startDate, endDate, allowedSensors,
                                                        sampleSize, trackerIds, connection, cb) {
    var groupCriteria = {};
    groupCriteria['_id'] = {tracker_id: '$device_id'};
    groupCriteria[_.camelCase(posReportField)] = {$avg: '$' + posReportField};

    var pipeline = [
        {
            '$match': {
                'time': {$gte: startDate, $lte: endDate},
                'device_id': {$in: trackerIds},
                'pos_status': {$ne: 'POS_INACCURATE'},
                'sensor_used': {$in: allowedSensors}
            }
        },
        {
            '$group': groupCriteria
        },
        {
            '$limit': sampleSize
        }
    ];
    db.aggregate(connection, COLLECTION, pipeline, cb);
}

function queryAveragePosReportStatistic(posReportField, allowedSensors, startDate, endDate, trackerId, connection, cb) {
    var groupCriteria = {};
    groupCriteria['_id'] = '$device_id';
    groupCriteria[posReportField] = {$avg: '$' + posReportField};

    var projectStage = {'_id': 1};
    projectStage[posReportField] = 1;

    var pipeline = [
        {
            '$match': {
                'time': {$gte: startDate, $lte: endDate},
                'device_id': trackerId,
                'pos_status': {$ne: 'POS_INACCURATE'},
                'sensor_used': {$in: allowedSensors}
            }
        },
        {
            '$group': groupCriteria
        },
        {
            '$project': projectStage
        }
    ];
    db.aggregate(connection, COLLECTION, pipeline, cb);
}

function queryNumberOfSensorReports(sensorUsed, startDate, endDate, trackerId, connection, cb) {
    var groupCriteria = {
        _id: '$device_id',
        no_cell_locates: {$sum: 1}
    };
    var projectStage = {'_id': 1, 'no_cell_locates': 1};

    var pipeline = [
        {
            '$match': {
                'time': {$gte: startDate, $lte: endDate},
                'device_id': trackerId,
                'sensor_used': sensorUsed
            }
        },
        {
            '$group': groupCriteria
        },
        {
            '$project': projectStage
        }
    ];
    db.aggregate(connection, COLLECTION, pipeline, cb);
}


