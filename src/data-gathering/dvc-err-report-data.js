var db = require('../db');
var async = require('async');
var util = require('../util');

var COLLECTION = 'dvc_err_reports';

module.exports = {
    insertErrorReportData: insertErrorReportData
};

function insertErrorReportData(dataInstances, cb) {
    console.log('Fetching device err report data...');

    async.waterfall([
        async.apply(getDeviceErrorReports, dataInstances),
        async.apply(util.mergeData, '_id', 'tracker_id', dataInstances)
    ], cb);
}

function getDeviceErrorReports(dataInstances, cb) {
    async.map(dataInstances, getDeviceErrorReportData, cb);
}

function getDeviceErrorReportData(data, cb) {
    getCountOfDeviceErrorReports(data.tracker_id, data.submit_date, function (err, result) {
        if (err) {
            return cb(err, result);
        }
        cb(err, result[0]);
    });
}

function getCountOfDeviceErrorReports(trackerId, submitDate, cb) {
    var pipeline = [
        {
            '$match': {
                device_id: trackerId,
                time_rcvd: {$lte: submitDate}
            }
        },
        {
            '$group': {_id: '$device_id', number_of_errors: {'$sum': 1}}
        }
    ];
    return db.aggregate(db.getTractiveDbConnection(), COLLECTION, pipeline, cb)
}