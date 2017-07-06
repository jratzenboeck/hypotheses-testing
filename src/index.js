var async = require('async');
var _ = require('lodash');
var db = require('./db');
var util = require('./util');
var serverCmdMetrics = require('./server-cmd-metrics');
var appUsage = require('./app-usage');
var clients = require('./clients');
var users = require('./users');
var dvcHwMetrics = require('./dvc-hw-metrics');
var dvcIdReports = require('./dvc-id-reports');

var commands = [
    'MSG_S2D_LIVE_TRACKING_MODE'
];
var startDate = new Date('2017-06-01T00:00:00Z');
var endDate = new Date('2017-06-08T00:00:00Z');

var currentDate = util.toDateString(new Date());
var filenames = ['data/cmdSuccessRate_' +  currentDate + '.csv', 'data/trackerUsage_' + currentDate + '.csv'];
//cmdSuccessRateVsX(filenames, mergeTrackerUsage);
//cmdTerminatedRateVsAppUsage(filenames);
//serverCmdMetrics.getServerCommandAverage(commands, 'cmd_delay_to_confirmed', startDate, endDate);
//batteryCriticalEventsVsAppUsage();
//systemShutDownEventsVsAppUsage();
//errLogCountVsAppUsage(filenames);
batteryLifeTimeVsTrackerUsage(null, mergeTrackerUsage);

function cmdSuccessRateVsX(filenames, vsFunction) {
    calcServerCmdMetricVsX(commands, 'cmd_success_rate', filenames, vsFunction);
}

function batteryLifeTimeVsTrackerUsage(filenames, vsFunction) {
    dvcHwMetrics.getAverageBatteryLifeTimeForTrackers(startDate, endDate, function(err, trackerBatteryLifeTimes) {
        if (err) {
            console.error(err.message);
            return;
        }
        vsFunction(trackerBatteryLifeTimes, 'batteryLifeTime', filenames);
    });
}

function cmdCancelledRateVsAppUsage(filenames) {
    calcServerCmdMetricVsX(commands, 'cmd_cancelled_rate', filenames, mergeAppUsage);
}

function cmdTerminatedRateVsAppUsage(filenames) {
    calcServerCmdMetricVsX(commands, 'cmd_terminated_rate', filenames, mergeAppUsage);
}

function batteryCriticalEventsVsAppUsage(filenames) {
    calcHwStatusMetricVsAppUsage(['BATT_CHARGING'], filenames, mergeAppUsage);
}

function systemShutDownEventsVsAppUsage(filenames) {
    calcHwStatusMetricVsAppUsage(['SYSTEM_SHUTDOWN'], filenames, mergeAppUsage);
}

function errLogCountVsAppUsage(filenames) {
    calcErrorLogCountVsAppUsage(filenames, mergeAppUsage);
}

function calcServerCmdMetricVsX(commands, cmdStatistic, filenames, vsFunction) {
    serverCmdMetrics.getServerCommandMetricsForUsers(
        commands, cmdStatistic, startDate,
        endDate, 1086,
        function (err, cmdStatResult) {
            if (err) {
                console.error(err.message);
                return;
            }
            vsFunction(cmdStatResult, _.camelCase(cmdStatistic), filenames);
        });
}

function calcHwStatusMetricVsAppUsage(hwStatus, filenames) {
    dvcHwMetrics.getHwStatusMetricsForUsers(
        hwStatus, startDate,
        endDate, 1086,
        function (err, hwStatusResult) {
            if (err) {
                console.error(err.message);
                return;
            }
            mergeAppUsage(hwStatusResult, _.camelCase(hwStatus + '_average'), filenames);
        });
}

function calcErrorLogCountVsAppUsage(filenames) {
    dvcHwMetrics.getErrorLogCount(startDate, endDate, 1086, function (err, errLogCount) {
        if (err) {
            console.error(err.message);
            return;
        }
        mergeAppUsage(errLogCount, 'sum_err_log_count', filenames);
    });
}

function mergeAppUsage(firstResult, firstResultFieldName, filenames) {
    appUsage.getAppUsages(util.getObjectIdsAsStringArray(firstResult, 'user_id'), clients.getClientIds,
        startDate, endDate, function (err, appUsages) {
            if (err) {
                console.error(err.message);
                return;
            }
            util.mergeAndPrint(firstResult, appUsages, firstResultFieldName, 'appOpenings', filenames)
        });
}

function mergeTrackerUsage(firstResult, firstResultFieldName, filenames) {
    dvcIdReports.getTrackerUsageMetrics(startDate, endDate,
        util.getObjectIdsAsStringArray(firstResult, 'tracker_id'),
        function (err, trackerUsage) {
            if (err) {
                console.error(err.message);
                return;
            }
            util.mergeAndPrint(firstResult, trackerUsage, firstResultFieldName,
                'trackerUsage', 'BatteryLifeTime', 'Tracker Usage', filenames);
        });
}