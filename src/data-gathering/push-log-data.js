var async = require('async');
var _ = require('lodash');
var util = require('../util');
var db = require('../db');
var geofenceData = require('./geofence-report-data');

var COLLECTION = 'push_logs';

module.exports = {
    insertAveragePushNotificationDelay: insertAveragePushNotificationDelay
};

function insertAveragePushNotificationDelay(inputData, cb) {
    async.waterfall([
        async.apply(getAveragePushNotificationDelay, inputData),
        async.apply(util.mergeData, 'tracker_id', 'tracker_id', inputData)
    ], cb);
}

function getAveragePushNotificationDelay(inputData, cb) {
    db.createMetricsConnection(function () {
        async.map(inputData, getAveragePushNotificationDelayForDevice, cb);
    });
}

function getAveragePushNotificationDelayForDevice(data, cb) {
    async.waterfall([
        async.apply(geofenceData.findGeofenceReports, data),
        async.apply(findAveragePushNotificationDelayForDevice, data)
    ], cb);
}

function findAveragePushNotificationDelayForDevice(data, geofenceReports, cb) {
    async.waterfall([
        async.apply(queryPushNotificationsForDevice, data.created_at, data.submit_date, data.tracker_id),
        async.apply(queryAveragePushNotificationDelay, geofenceReports)
    ], cb);
}

function queryPushNotificationsForDevice(intervalStart, intervalEnd, trackerId, cb) {
    return db.aggregate(db.getMetricsDbConnection(), COLLECTION,
        [
            {
                '$match': {
                    created_at: {$gte: intervalStart, $lte: intervalEnd},
                    'message.message': {$in: ['geofence_in', 'geofence_out']},
                    'message.reference': trackerId
                }
            },
            {
                '$project': {
                    _id: 0,
                    created_at: 1,
                    trigger: '$message.message',
                    tracker_id: '$message.reference'
                }
            }
        ], function(err, results) {
            cb(err, results);
        });
}

function queryAveragePushNotificationDelay(geofenceReports, notifications, cb) {
    var timeDifferences = [];
    _.forEach(notifications, function (notification) {
        var smallestDiff = Number.MAX_VALUE;
        _.forEach(_.filter(geofenceReports, function (geofenceReport) {
            _.includes(geofenceReport.trigger, _.upperCase(notification.trigger))
        }), function (report) {
            var currentDiff = notification.created_at - report.time;
            if (currentDiff < smallestDiff) {
                smallestDiff = currentDiff;
            }
        });
        timeDifferences.push({tracker_id: notification.tracker_id, delay: smallestDiff});
    });
    return cb(null, {tracker_id: timeDifferences[0].tracker_id, delay: _.mean(_.map(timeDifferences, function(o) {return o.delay}))} || undefined);
}
