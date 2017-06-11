var async = require('async');
var _ = require('lodash');
var db = require('./db');
var util = require('./util');
var serverCmdMetrics = require('./server-cmd-metrics');
var appUsage = require('./app-usage');
var clients = require('./clients');

var commands = [
    'MSG_S2D_LIVE_TRACKING_MODE'
];
var startDate = new Date("2017-06-01T00:00:00.000Z");
var endDate = new Date("2017-06-11T00:00:00.000Z");

var filenames = ['data/cmdCancelledRate.csv', 'data/appUsageCancelled.csv'];
cmdTerminatedRateVsAppUsage();
//serverCmdMetrics.getServerCommandAverage(commands, 'cmd_delay_to_confirmed', startDate, endDate);

function cmdSuccessRateVsAppUsage(filenames) {
    calcServerCmdMetricVsAppUsage(commands, 'cmd_success_rate', filenames);
}

function cmdCancelledRateVsAppUsage(filenames) {
    calcServerCmdMetricVsAppUsage(commands, 'cmd_cancelle_rate', filenames);
}

function cmdTerminatedRateVsAppUsage(filenames) {
    calcServerCmdMetricVsAppUsage(commands, 'cmd_terminated_rate', filenames);
}

function calcServerCmdMetricVsAppUsage(commands, cmdStatistic, filenames) {
    var commandRateType = 'cmd_success_rate';
    serverCmdMetrics.getServerCommandMetricsForUsers(
        commands, cmdStatistic, startDate,
        endDate, 1086,
        function (err, cmdStatistic) {
            if (err) {
                console.error(err.message);
                return;
            }
            var userIds = util.getObjectIdsAsStringArray(cmdStatistic);
            appUsage.getAppUsages(userIds, clients.getClientIds, startDate, endDate, function (err, appUsages) {
                if (err) {
                    console.error(err.message);
                    return;
                }
                util.mergeArraysById(cmdStatistic, appUsages, _.camelCase(commandRateType), 'appOpenings', function (res1, res2) {
                    if (!!filenames) {
                        util.writeResultsToFile(res1, res2, filenames[0], filenames[1]);
                    } else {
                        util.writeResultsToConsole(res1, res2);
                    }
                });
            });
        });
}
