var async = require('async');
var util = require('../util');
var db = require('../db');

var COLLECTION = 'geofence_reports';

module.exports = {
    findGeofenceReports: findGeofenceReports
};

function findGeofenceReports(data, cb) {
    queryGeofenceReports(data.created_at, data.submit_date, data.tracker_id, cb);
}
function queryGeofenceReports(startDate, endDate, trackerId, cb) {
    db.find(db.getTractiveDbConnection(), COLLECTION,
        {device_id: trackerId, time: {$gte: startDate, $lte: endDate}},
        {_id: 0, device_id: 1, trigger: 1, time: 1}, cb);
}