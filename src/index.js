var db = require('./db');
var async = require('async');

getAppUsageServerCommandStatistic();

function getAppUsageServerCommandStatistic() {
    findCommandSuccessRate(function(err, commandSuccessRates) {
        findAppUsages(commandSuccessRates, function(err, appUsages) {
           mergeArraysById(commandSuccessRates, appUsages, function(res1, res2) {
              console.log('Cmd Success Rates\n\n');
              res1.forEach(function(cmdSr) {
                  console.log(cmdSr);
              });
              console.log('App openings\n\n');
              res2.forEach(function(appU) {
                  console.log(appU);
              });
           });
        });
    });
}

function mergeArraysById(arr1, arr2, cb) {
    var resultArr1 = [];
    var resultArr2 = [];

    for (var i = 0; i < arr1.length; i++) {
        for (var j = 0; j < arr2.length; j++) {
            if (arr1[i]._id.toString() === arr2[j]._id.toString()) {
                resultArr1.push(Math.round(arr1[i].cmdSuccessRate * 1000) / 1000);
                resultArr2.push(arr2[j].appOpenings);
            }
        }
    }
    cb(resultArr1, resultArr2);
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
                return cb(err, extractArrayContent(result));
            });
}

function findAppUsages(commandSuccessRates, cb) {
    async.series({
        clientIds: getClientIds
    }, function (err, result) {
        async.waterfall([
            async.apply(db.createConnection, 'tractivedb_metrics'),
            async.apply(queryAppUsages, extractArrayContent(commandSuccessRates), result.clientIds)
        ], function (err, appUsages) {
            cb(err, appUsages);
        });
    });
}

function queryAppUsages(userIds, clientIds, connection, cb) {
    connection.collection('mobile_events').aggregate([
        {
            '$match': {
                user_id: {$in: userIds},
                created_at: {
                    $gte: new Date("2017-05-01T00:00:00.000Z"),
                    $lt: new Date("2017-06-01T00:00:00.000Z")
                },
                event: 'app_foreground',
                client_id: {$in: clientIds}
            }
        },
        {'$group': {_id: '$user_id', appOpenings: {$sum: 1}}},
        {'$match': {appOpenings: {$gte: 1}}}
    ], function (err, result) {
        connection.close();
        return cb(err, result);
    });
}

function findCommandSuccessRate(cb) {
    async.series({
       userIds: getRandomUserIds
    }, function (err, result) {
        async.waterfall([
            async.apply(db.createConnection, 'tractivedb_metrics'),
            async.apply(queryCommandSuccessRate, result.userIds)
        ], function (err, result) {
            cb(err, result);
        });
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
        {'$group': {_id: '$requested_by', cmdSuccessRate: {$avg: '$cmd_success_rate'}}},
        {'$limit': 1086}
    ], function (err, result) {
        connection.close();
        cb(err, result);
    });
}






