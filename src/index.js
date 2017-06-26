var async = require('async');
var _ = require('lodash');
var db = require('./db');
var util = require('./util');
var serverCmdMetrics = require('./server-cmd-metrics');
var appUsage = require('./app-usage');
var clients = require('./clients');
var users = require('./users');
var dvcHwMetrics = require('./dvc-hw-metrics');

var commands = [
    'MSG_S2D_BUZZER_CONTROL',
    'MSG_S2D_ED_CONTROL',
    'MSG_S2D_LIVE_TRACKING_MODE'
];
var startDate = new Date('2017-06-01T00:00:00Z');
var endDate = new Date('2017-06-19T00:00:00Z');

var filenames = ['data/errLogCount.csv', 'data/appUsageErrLogCount.csv'];
cmdSuccessRateVsAppUsage();
//cmdTerminatedRateVsAppUsage(filenames);
//serverCmdMetrics.getServerCommandAverage(commands, 'cmd_delay_to_confirmed', startDate, endDate);
//batteryCriticalEventsVsAppUsage();
//systemShutDownEventsVsAppUsage();
//errLogCountVsAppUsage(filenames);

function cmdSuccessRateVsAppUsage(filenames) {
    calcServerCmdMetricVsAppUsage(commands, 'cmd_success_rate', filenames);
}

function cmdCancelledRateVsAppUsage(filenames) {
    calcServerCmdMetricVsAppUsage(commands, 'cmd_cancelled_rate', filenames);
}

function cmdTerminatedRateVsAppUsage(filenames) {
    calcServerCmdMetricVsAppUsage(commands, 'cmd_terminated_rate', filenames);
}

function batteryCriticalEventsVsAppUsage(filenames) {
    calcHwStatusMetricVsAppUsage(['BATT_CHARGING'], filenames);
}

function systemShutDownEventsVsAppUsage(filenames) {
    calcHwStatusMetricVsAppUsage(['SYSTEM_SHUTDOWN'], filenames);
}

function errLogCountVsAppUsage(filenames) {
    calcErrorLogCountVsAppUsage(filenames);
}

function calcServerCmdMetricVsAppUsage(commands, cmdStatistic, filenames) {
    serverCmdMetrics.getServerCommandMetricsForUsers(
        commands, cmdStatistic, startDate,
        endDate, 1086,
        function (err, cmdStatResult) {
            if (err) {
                console.error(err.message);
                return;
            }
            mergeAppUsage(cmdStatResult, _.camelCase(cmdStatistic), filenames);
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
    dvcHwMetrics.getErrorLogCount(startDate, endDate, 1086, function(err, errLogCount) {
       if (err) {
           console.error(err.message);
           return;
       }
       mergeAppUsage(errLogCount, 'sum_err_log_count', filenames);
    });
}

function mergeAppUsage(firstResult, firstResultFieldName, filenames) {
    appUsage.getAppUsages(util.getObjectIdsAsStringArray(firstResult, '_id'), clients.getClientIds,
        startDate, endDate, function (err, appUsages) {
            if (err) {
                console.error(err.message);
                return;
            }
            util.mergeArraysById(firstResult, appUsages, firstResultFieldName, 'appOpenings',
                function (res1, res2) {
                    if (!!filenames) {
                        util.writeResultsToFile(res1, res2, filenames[0], filenames[1]);
                    } else {
                        util.writeResultsToConsole(res1, res2);
                    }
                });
        });
}