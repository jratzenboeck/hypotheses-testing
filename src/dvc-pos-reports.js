var async = require('async');
var _ = require('lodash');
var db = require('./db');
var util = require('./util');
var users = require('./users');
var trackers = require('./trackers');

var COLLECTION = 'dvc_pos_reports';

module.exports = {
  getAveragePosReportStatisticValueForTrackers: getAveragePosReportStatisticValueForTrackers
};

function getAveragePosReportStatisticValueForTrackers(posReportField, startDate, endDate, allowedSensors, sampleSize, cb) {
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
