var _ = require('lodash');
var async = require('async');
var survey = require('./customer-survey-data');
var idReport = require('./id-report-data');
var posReport = require('./pos-report-data');
var serverCmd = require('./server-cmd-data');
var subscription = require('./subscription-data');
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
            //var headers = ['_id', 'submit_date', 'rating', 'recommendation_score', 'user_id', 'tracker_id', 'created_at', 'cmd_success_rate', 'cmd_cancelled_rate'];
            //util.writeDataToFile(finalResultData, headers, 'ml_data/test_data.csv');
        }
    });
}

function expandData(customerSurveyData, cb) {
    async.waterfall([
        async.apply(subscription.insertStartDate, customerSurveyData),
        idReport.insertNumberOfDaysInUse
        //serverCmd.insertServerCmdMetricsData
    ], cb);
}






