var async = require('async');
var posReports = require('../dvc-pos-reports');
var util = require('./utility');

module.exports = {
    insertPosReportData: insertPosReportData
};

function insertPosReportData(dataInstances, cb) {
    async.waterfall([
        async.apply(insertAverageGsmRssi, dataInstances)
        // insertAveragePosUncertainty,
        // insertNumberOfCellLocates,
        // insertAverageNumberOfSatellites
    ], cb);
}

function insertAverageGsmRssi(dataInstances, cb) {
    util.insertAverageStatisticValue(dataInstances, getAverageGsmRssi, cb);
}

function getAverageGsmRssi(dataInstances, cb) {
    async.map(dataInstances, async.apply(posReports.getAveragePosReportStatisticForTracker, 'gsm_rssi', ['GPS']), cb);
}

function insertAveragePosUncertainty(dataInstances, cb) {
    util.insertAverageStatisticValue(dataInstances, getAveragePosUncertainty, cb);
}

function getAveragePosUncertainty(dataInstances, cb) {
    async.map(dataInstances, async.apply(posReports.getAveragePosReportStatisticForTracker, 'pos_uncertainty', ['GPS']), cb);
}

function insertNumberOfCellLocates(dataInstances, cb) {
    util.insertAverageStatisticValue(dataInstances, getNumberOfCellLocates, cb);
}

function getNumberOfCellLocates(dataInstances, cb) {
    async.map(dataInstances, async.apply(posReports.getNumberOfSensorReports, 'CELL_LOCATE'), cb);
}

function insertAverageNumberOfSatellites(dataInstances, cb) {
    util.insertAverageStatisticValue(dataInstances, getAverageNumberOfSatellites, cb);
}

function getAverageNumberOfSatellites(dataInstances, cb) {
    async.map(dataInstances, async.apply(posReports.getAveragePosReportStatisticForTracker, 'no_of_sat', ['GPS']), cb);
}
