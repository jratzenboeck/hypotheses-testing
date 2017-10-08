var async = require('async');
var util = require('../util');
var db = require('../db');
var _ = require('lodash');

var COLLECTION = 'geofence_reports';

module.exports = {
    findGeofenceReports: findGeofenceReports,
    insertGeofenceReportData: insertGeofenceReportData
};

function findGeofenceReports(data, cb) {
    queryGeofenceReports(data.created_at, data.submit_date, data.tracker_id, cb);
}
function queryGeofenceReports(startDate, endDate, trackerId, cb) {
    db.find(db.getTractiveDbConnection(), COLLECTION,
        {device_id: trackerId, time: {$gte: startDate, $lte: endDate}},
        {_id: 0, device_id: 1, trigger: 1, time: 1}, cb);
}

function insertGeofenceReportData(dataInstances, cb) {
    console.log('Fetching geofence report data...');

    async.waterfall([
        async.apply(getGeofenceReportData, dataInstances),
        async.apply(util.mergeData, '_id', 'tracker_id', dataInstances)
    ], cb);
}

function getGeofenceReportData(dataInstances, cb) {
    async.map(dataInstances, getGeofenceReportDataForDevice, cb);
}

function getGeofenceReportDataForDevice(data, cb) {
    queryGeofenceReportData(data.tracker_id, data.submit_date, function (err, result) {
        if (err) {
            return cb(err, result);
        }
        var geofenceData = {_id: data.tracker_id, number_of_reports: -1};
        if (!!result && result.length > 0) {
            geofenceData.number_of_geofence_reports = result[0].number_of_reports;
        }
        cb(err, geofenceData);
    });
}

function queryGeofenceReportData(trackerId, submitDate, cb) {
    var pipeline = [
        {
            '$match': {
                device_id: trackerId,
                time: {$lte: submitDate}
            }
        },
        {
            '$group': {_id: '$device_id', number_of_reports: {'$sum': 1}}
        }
    ];
    return db.aggregate(db.getTractiveDbConnection(), COLLECTION, pipeline, cb);
}