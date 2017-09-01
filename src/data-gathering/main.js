var _ = require('lodash');
var async = require('async');
var survey = require('./customer-survey-data');
var idReport = require('./id-report-data');
var posReport = require('./pos-report-data');
var serverCmd = require('./server-cmd-data');
var subscription = require('./subscription-data');

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
        async.apply(subscription.insertStartDate, customerSurveyData),

        serverCmd.insertServerCmdMetricsData
    ], cb);
}






