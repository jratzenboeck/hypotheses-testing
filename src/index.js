var db = require('./db');
var async = require('async');

getAppUsageServerCommandStatistic();

function getAppUsageServerCommandStatistic() {
    async.waterfall([
        findAppUsages,
        findCommandSuccessRate
    ], function (err, result) {
        console.log(result);
    });
}

function getRandomUserIds(cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        queryRandomUserIds
    ], function (err, result) {
        cb(err, result);
    });
}

function queryRandomUserIds(connection, cb) {
    connection.collection('ppl_subscriptions').aggregate([
        {'$match': {status: 'ACTIVE', created_at: {$lte: new Date("2017-05-01T00:00:00.000Z")}}},
        {'$group': {_id: '$user_id', countTrackers: {$sum: 1}}},
        {'$match': {countTrackers: 1}},
        {'$skip': Math.random() * 100},
        {'$limit': 5000}
    ], function (err, result) {
        connection.close();
        return cb(err, extractArrayContent(result));
    });
}

function extractArrayContent(jsonArray) {
    var extractedArray = [];
    jsonArray.forEach(function (jsonData) {
        extractedArray.push(jsonData._id);
    });
    return extractedArray;
}

function getClientIds(cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        queryClientIds
    ], function (err, result) {
        cb(err, result);
    });
}

function queryClientIds(connection, cb) {
    connection.collection('clients')
        .find({app_name: 'tractivegps'}, {_id: 1})
        .toArray(
            function (err, result) {
                connection.close();
                var clientIds = [];
                result.forEach(function (client) {
                    clientIds.push(client._id)
                });
                return cb(err, clientIds);
            });
}

function findAppUsages(cb) {
    async.parallel({
        userIds: getRandomUserIds,
        clientIds: getClientIds
    }, function (err, result) {
        async.waterfall([
            async.apply(db.createConnection, 'tractivedb_metrics'),
            async.apply(getAppUsages, result.userIds, result.clientIds)
        ], function (err, appUsages) {
            //console.log(appUsages);
            cb(err, appUsages);
        });
    });
}

function getAppUsages(userIds, clientIds, connection, cb) {
    connection.collection('mobile_events').aggregate([
        {
            '$match': {
                user_id: {$in: userIds},
                created_at: {
                    $gte: new Date("2017-05-01T00:00:00.000Z")

                },
                event: 'app_foreground',
                client_id: {$in: clientIds}
            }
        },
        {'$group': {_id: '$user_id', countEvents: {$sum: 1}}},
        {'$match': {countEvents: {$gte: 1}}},
        {'$limit': 1086}
    ], function (err, result) {
        connection.close();
        return cb(err, result);
    });
}

function findCommandSuccessRate(appUsages, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb_metrics'),
        async.apply(queryCommandSuccessRate, extractArrayContent(appUsages))
    ], function (err, result) {
        cb(err, result);
    });
}

function queryCommandSuccessRate(userIds, connection, cb) {
    var commands = [
        "MSG_S2D_POS_REPORT_REQUEST",
        "MSG_S2D_BUZZER_CONTROL",
        "MSG_S2D_LED_CONTROL",
        "MSG_S2D_LIVE_TRACKING_MODE"
    ];
    connection.collection('server_command_metrics').aggregate([
        {
            '$match': {
                msg_name: {$in: commands},
                requested_by: {$in: userIds},
                created_at: {
                    $gte: new Date("2017-05-01T00:00:00.000Z"),
                    $lt: new Date("2017-06-01T00:00:00.000Z")
                }
            }
        },
        {'$group': {_id: '$requested_by', cmdSuccessRate: {$avg: '$cmd_success_rate'}}}
    ], function (err, result) {
        connection.close();
        cb(err, result);
    });
}






