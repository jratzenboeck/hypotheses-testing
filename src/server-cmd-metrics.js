var async = require('async');
var _ = require('lodash');
var db = require('./db');
var util = require('./util');
var users = require('./users');

var COLLECTION = 'server_command_metrics';

module.exports = {
    getServerCommandMetricsForUsers: getServerCommandMetricsForUsers,
    getServerCommandMetricsForTracker: getServerCommandMetricsForTracker
};

function getServerCommandMetricsForTracker(commands, cmdStatistic, data, cb) {
    queryServerCommandMetricsForTracker(commands, cmdStatistic, data.created_at, data.submit_date, data.tracker_id,
     function(err, results) {
        cb(err, results[0]);
    });
}

function queryServerCommandMetricsForTracker(commands, cmdStatistic, startDate, endDate, trackerId, cb) {
    var matchCriteria = {};
    matchCriteria[cmdStatistic] = {'$exists': true};
    matchCriteria['msg_name'] = {$in: commands};
    matchCriteria['mode_on'] = true;
    matchCriteria['device_id'] = trackerId;
    matchCriteria['requested_at'] = {'$gte': startDate, '$lt': endDate};

    var groupStage = {_id: '$device_id'};
    groupStage[cmdStatistic] = {$avg: '$' + cmdStatistic};

    var pipeline = [
        {'$match': matchCriteria},
        {'$group': groupStage}
    ];

    db.aggregate(db.getMetricsDbConnection(), COLLECTION, pipeline, cb);
}

function getServerCommandMetricsForUsers(commands, cmdStatistic, startDate, endDate, sampleSize, cb) {
    async.series({
        userIds: async.apply(users.getRandomSubscriptions, startDate)
    }, function (err, result) {
        async.waterfall([
            async.apply(db.createConnection, 'tractivedb_metrics'),
            async.apply(queryServerCommandMetricsForUsers, commands, cmdStatistic,
                        util.getObjectIdsAsStringArray(result.userIds, 'user_id'), startDate, endDate, sampleSize)
        ], function (err, result) {
            cb(err, result);
        });
    });
}

function queryServerCommandMetricsForUsers(commands, cmdStatistic, userIds, startDate, endDate, sampleSize, connection, cb) {
    var matchCriteria = {};
    matchCriteria[cmdStatistic] = {'$exists': true};
    matchCriteria['msg_name'] = {$in: commands};
    matchCriteria['mode_on'] = true;
    matchCriteria['requested_by'] = {$in: userIds};
    matchCriteria['requested_at'] = {'$gte': startDate, '$lt': endDate};

    var groupStage = {};
    groupStage['_id'] = {user_id: '$requested_by', tracker_id: '$device_id'};
    groupStage[_.camelCase(cmdStatistic)] = {$avg: '$' + cmdStatistic};

    var pipeline = [
        {'$match': matchCriteria},
        {'$group': groupStage},
        {'$limit': sampleSize}
    ];

    db.aggregate(connection, 'server_command_metrics', pipeline, cb);
}

function getServerCommandAverage(commands, cmdStatistic, startDate, endDate) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb_metrics'),
        async.apply(queryCommandAverage, commands, cmdStatistic, startDate, endDate)
    ], function (err, result) {
        if (err) {
            console.error(err.message);
        } else {
            console.log(result);
        }
    });
}

function queryCommandAverage(commands, cmdStatistic, startDate, endDate, connection, cb) {
    var matchCriteria = {};
    matchCriteria[cmdStatistic] = {'$exists': true};
    matchCriteria['msg_name'] = {$in: commands};
    matchCriteria['requested_at'] = {'$gte': startDate, '$lt': endDate};

    var groupStage = {};
    groupStage['_id'] = null;
    groupStage[_.camelCase(cmdStatistic + '_avg')] = {$avg: '$' + cmdStatistic};

    var pipeline = [
        {'$match': matchCriteria},
        {'$group': groupStage}
    ];

    db.aggregate(connection, 'server_command_metrics', pipeline, cb);
}