var async = require('async');
var serverCmdMetrics = require('../server-cmd-metrics');
var util = require('./utility');
var db = require('../db');

module.exports = {
    insertServerCmdMetricsData: insertServerCmdMetricsData
};

function insertAverageCmdSuccessRate(dataInstances, cb) {
    console.log('Fetching cmd_success_rate...');
    util.insertAverageStatisticValue(dataInstances, getAverageServerCmdSuccessRate, cb);
}

function getAverageServerCmdSuccessRate(dataInstances, cb) {
    getAverageServerCmdRate(dataInstances, ['MSG_S2D_LIVE_TRACKING_MODE'], 'cmd_success_rate', cb);
}

function getAverageServerCmdRate(dataInstances, commands, cmdStatistic, cb) {
    async.map(dataInstances, async.apply(serverCmdMetrics.getServerCommandMetricsForTracker, commands, cmdStatistic), cb)
}

function insertAverageCmdCancelledRate(dataInstances, cb) {
    console.log('Fetching cmd_cancelled_rate...');
    util.insertAverageStatisticValue(dataInstances, getAverageCmdCancelledRate, cb);
}

function getAverageCmdCancelledRate(dataInstances, cb) {
    getAverageServerCmdRate(dataInstances, ['MSG_S2D_LIVE_TRACKING_MODE'], 'cmd_cancelled_rate', cb);
}

function insertServerCmdMetricsData(dataInstances, cb) {
    db.createMetricsConnection(function() {
        async.waterfall([
            async.apply(insertAverageCmdSuccessRate, dataInstances),
            insertAverageCmdCancelledRate,
            insertAverageCmdTerminatedRate,
            insertAverageCmdDelayToConfirmed,
            insertAverageCmdDelayToPosAny,
            insertAverageCmdDelayToPosNew,
            insertAverageCmdDelayToCommanded,
            insertAverageCmdDelayToIdReport,
            insertAverageCmdDuration
        ], cb);
    });
}

function insertAverageCmdTerminatedRate(dataInstances, cb) {
    console.log('Fetching cmd_terminated_rate...');
    util.insertAverageStatisticValue(dataInstances, getAverageCmdTerminatedRate, cb);
}

function getAverageCmdTerminatedRate(dataInstances, cb) {
    getAverageServerCmdRate(dataInstances, ['MSG_S2D_LIVE_TRACKING_MODE'], 'cmd_terminated_rate', cb);
}

function insertAverageCmdDelayToConfirmed(dataInstances, cb) {
    console.log('Fetching cmd_delay_confirmed...');
    util.insertAverageStatisticValue(dataInstances, getAverageCmdDelayToConfirmed, cb);
}

function getAverageCmdDelayToConfirmed(dataInstances, cb) {
    getAverageServerCmdRate(dataInstances, ['MSG_S2D_LIVE_TRACKING_MODE'], 'cmd_delay_to_confirmed', cb);
}

function insertAverageCmdDelayToPosAny(dataInstances, cb) {
    console.log('Fetching cmd_delay_to_pos_any...');
    util.insertAverageStatisticValue(dataInstances, getAverageCmdDelayToPosAny, cb);
}

function getAverageCmdDelayToPosAny(dataInstances, cb) {
    getAverageServerCmdRate(dataInstances, ['MSG_S2D_LIVE_TRACKING_MODE'], 'cmd_delay_to_pos_any', cb);
}

function insertAverageCmdDelayToPosNew(dataInstances, cb) {
    console.log('Fetching cmd_delay_to_pos_new...');
    util.insertAverageStatisticValue(dataInstances, getAverageCmdDelayToPosNew, cb);
}

function getAverageCmdDelayToPosNew(dataInstances, cb) {
    getAverageServerCmdRate(dataInstances, ['MSG_S2D_LIVE_TRACKING_MODE'], 'cmd_delay_to_pos_new', cb)
}

function insertAverageCmdDelayToIdReport(dataInstances, cb) {
    console.log('Fetching cmd_delay_to_id_report...');
    util.insertAverageStatisticValue(dataInstances, getAverageCmdDelayToIdReport, cb);
}

function getAverageCmdDelayToIdReport(dataInstances, cb) {
    getAverageServerCmdRate(dataInstances, ['MSG_S2D_LIVE_TRACKING_MODE'], 'cmd_delay_to_id_report', cb)
}

function insertAverageCmdDelayToCommanded(dataInstances, cb) {
    console.log('Fetching cmd_delay_to_commanded...');
    util.insertAverageStatisticValue(dataInstances, getAverageCmdDelayToCommanded, cb);
}

function getAverageCmdDelayToCommanded(dataInstances, cb) {
    getAverageServerCmdRate(dataInstances, ['MSG_S2D_LIVE_TRACKING_MODE'], 'cmd_delay_to_commanded', cb)
}

function insertAverageCmdDuration(dataInstances, cb) {
    console.log('Fetching cmd_duration...');
    util.insertAverageStatisticValue(dataInstances, getAverageCmdDuration, cb);
}

function getAverageCmdDuration(dataInstances, cb) {
    getAverageServerCmdRate(dataInstances, ['MSG_S2D_LIVE_TRACKING_MODE'], 'cmd_duration', cb)
}

