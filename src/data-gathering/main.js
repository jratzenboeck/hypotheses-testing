var _ = require('lodash');
var async = require('async');
var survey = require('./customer-survey-data');
var idReport = require('./id-report-data');
var posReport = require('./pos-report-data');
var serverCmd = require('./server-cmd-data');
var subscription = require('./subscription-data');
var user = require('./user-data');
var customerSupport = require('./customer-support-data');
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
            var headers = _.keys(_.first(_.filter(finalResultData, function(data) { return data.ticket_count > 0})));
            util.writeDataToFile(finalResultData, headers, 'ml_data/test_data.csv');
        }
    });
}

function expandData(customerSurveyData, cb) {
    async.waterfall([
        async.apply(user.insertUserEmail, customerSurveyData),
        subscription.insertStartDate,
        customerSupport.insertCustomerSupportData
        // posReport.insertPosReportData,
        // idReport.insertNumberOfDaysInUse,
        // serverCmd.insertServerCmdMetricsData
    ], cb);
}






