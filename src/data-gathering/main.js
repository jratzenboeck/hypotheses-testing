var _ = require('lodash');
var async = require('async');
var survey = require('./customer-survey-data');
var posReports = require('../dvc-pos-reports');
var subscriptions = require('./subscriptions');
var util = require('../util');

insertCustomerSurveyData();

function insertCustomerSurveyData() {
    async.waterfall([
        survey.getUserSurveyData,
        expandData
    ], function (err, finalResultData) {
        if (err) {
            console.log('An error occurred, message: ' + err.message);
        } else {
            console.log(JSON.stringify(finalResultData));
            console.log(finalResultData.length);
        }
    });
}

function expandData(customerSurveyData, cb) {
    async.waterfall([
        async.apply(insertStartDate, customerSurveyData),
        insertAverageGsmRssi,
        insertAveragePosUncertainty,
        insertNumberOfCellLocates
    ], cb);
}

function insertStartDate(dataInstances, cb) {
    async.waterfall([
        async.apply(getStartDate, dataInstances),
        async.apply(util.mergeData, 'user_id', 'user_id', dataInstances)
    ], cb);
}

function getStartDate(dataInstances, cb) {
    async.map(dataInstances, subscriptions.getSubscriptionActivationTime, cb);
}

function insertAverageGsmRssi(dataInstances, cb) {
    async.waterfall([
        async.apply(getAverageGsmRssi, dataInstances),
        async.apply(util.mergeData, '_id', 'tracker_id', dataInstances)
    ], cb);
}

function getAverageGsmRssi(dataInstances, cb) {
    async.map(dataInstances, async.apply(posReports.getAveragePosReportStatisticForTracker, 'gsm_rssi', ['GPS']), cb);
}

function insertAveragePosUncertainty(dataInstances, cb) {
    async.waterfall([
        async.apply(getAveragePosUncertainty, dataInstances),
        async.apply(util.mergeData, '_id', 'tracker_id', dataInstances)
    ], cb);
}

function getAveragePosUncertainty(dataInstances, cb) {
    async.map(dataInstances, async.apply(posReports.getAveragePosReportStatisticForTracker, 'pos_uncertainty', ['GPS']), cb);
}

function insertNumberOfCellLocates(dataInstances, cb) {
    async.waterfall([
        async.apply(getNumberOfCellLocates, dataInstances),
        async.apply(util.mergeData, '_id', 'tracker_id', dataInstances)
    ], cb);
}

function getNumberOfCellLocates(dataInstances, cb) {
    async.map(dataInstances, async.apply(posReports.getNumberOfSensorReports, 'CELL_LOCATE'), cb);
}


