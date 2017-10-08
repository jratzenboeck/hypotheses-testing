var _ = require('lodash');
var async = require('async');
var survey = require('./customer-survey-data');
var idReport = require('./id-report-data');
var posReport = require('./pos-report-data');
var serverCmd = require('./server-cmd-data');
var subscription = require('./subscription-data');
var device = require('./device-data');
var user = require('./user-data');
var customerSupport = require('./customer-support-data');
var util = require('../util');
var dvcErrors = require('./dvc-err-report-data');
var geofence_report_data = require('./geofence-report-data');
var geofence_data = require('./geofence-data');

insertCustomerSurveyData();

function insertCustomerSurveyData() {
    async.waterfall([
        survey.getUserSurveyData,
        expandData
    ], function (err, finalResultData) {
        if (err) {
            console.log('An error occurred, message: ' + err.message + ', full error: ' + err);
        } else {
            console.log(JSON.stringify(finalResultData));
            console.log(finalResultData.length);
            var headers = initCsvHeaderData(finalResultData);
            util.writeDataToFile(finalResultData, headers, 'ml_data/ML_Data_new.csv');
        }
    });
}

function initCsvHeaderData(dataRows) {
    return _.keys(_.omit(_.head(dataRows), ['zendesk_user']));
}

function expandData(customerSurveyData, cb) {
    async.waterfall([
        async.apply(geofence_data.insertGeofenceData, customerSurveyData),
        subscription.insertSubscriptionData,
        //device.insertDeviceData,
        //customerSupport.insertCustomerSupportData,
        posReport.insertPosReportData,
        idReport.insertNumberOfDaysInUse,
        serverCmd.insertServerCmdMetricsData
    ], cb);
}






