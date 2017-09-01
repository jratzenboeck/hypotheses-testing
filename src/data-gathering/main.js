var _ = require('lodash');
var async = require('async');
var survey = require('./customer-survey-data');
var posReports = require('../dvc-pos-reports');
var subscriptions = require('./subscriptions');

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
        insertAverageGsmRssi
    ], cb);
}

function insertStartDate(dataInstances, cb) {
    async.waterfall([
        async.apply(getStartDate, dataInstances),
        async.apply(mergeData, 'user_id', 'user_id', dataInstances)
    ], cb);
}

function getStartDate(dataInstances, cb) {
    async.map(dataInstances, subscriptions.getSubscriptionActivationTime, cb);
}

function mergeData(mergeAttributeSrc, mergeAttributeDest, destinationData, sourceData, cb) {
    for (var i = 0; i < destinationData.length; i++) {
        for (var j = 0; j < sourceData.length; j++) {
            if (!sourceData[j]) {
                continue;
            }
            if (sourceData[j][mergeAttributeSrc].toString() === destinationData[i][mergeAttributeDest].toString()) {
                destinationData[i] = _.assign(destinationData[i], sourceData[j]);
            }
        }
    }
    cb(null, destinationData);
}

function insertAverageGsmRssi(dataInstances, cb) {
    async.waterfall([
        async.apply(getAverageGsmRssi, dataInstances),
        async.apply(mergeData, '_id', 'tracker_id', dataInstances)
    ], cb);
}

function getAverageGsmRssi(dataInstances, cb) {
    async.map(dataInstances, async.apply(posReports.getAveragePosReportStatisticForTracker, 'gsm_rssi', ['GPS']), cb);
}


