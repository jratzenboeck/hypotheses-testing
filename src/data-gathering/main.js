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
var geofenceReportData = require('./geofence-report-data');
var geofenceData = require('./geofence-data');
var notificationData = require('./notification-data');
var resourceSocialData = require('./resource-social');
var resourceData = require('./resource-data');
var shareData = require('./share-data');
var petSurveyData = require('./pet-survey-data');

insertCustomerSurveyData();

function insertCustomerSurveyData() {
    async.waterfall([
        survey.getUserSurveyDataUnique,
        expandData
    ], function (err, finalResultData) {
        if (err) {
            console.log('An error occurred, message: ' + err.message + ', full error: ' + err);
        } else {
            console.log(JSON.stringify(finalResultData));
            console.log(finalResultData.length);
            var headers = initCsvHeaderData(finalResultData);
            util.writeDataToFile(finalResultData, headers, 'ml_data/data' + new Date().getTime() + '.csv');
        }
    });
}

function initCsvHeaderData(dataRows) {
    return _.keys(_.omit(_.head(dataRows), ['zendesk_user', 'submit_date', 'created_at', '_id', 'email']));
}

function expandData(customerSurveyData, cb) {
    async.waterfall([
        async.apply(user.insertUserData, customerSurveyData),
        customerSupport.insertCustomerSupportData,
        subscription.insertSubscriptionData,
        device.insertDeviceData,
        petSurveyData.insertPetSurveyData,
        shareData.insertReceivedSharesForUsers,
        shareData.insertSentSharesForTrackers,
        resourceData.insertResourceData,
        resourceSocialData.insertResourceCommentsData,
        resourceSocialData.insertResourceLikesData,
        notificationData.insertNotificationsData,
        geofenceData.insertGeofenceData,
        geofenceReportData.insertGeofenceReportData,
        posReport.insertPosReportData,
        idReport.insertNumberOfDaysInUse,
        serverCmd.insertServerCmdMetricsData
    ], cb);
}






