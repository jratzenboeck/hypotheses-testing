var db = require('./db');
var async = require('async');
var fs = require('fs');
var csvWriter = require('csv-write-stream');

calcAppUsageServerCommandStatistic('data/cmdCancelledRate.csv', 'data/appUsageCancelled.csv');
//getCommandDelayAverage('cmd_delay_to_confirmed');

function calcAppUsageServerCommandStatistic(filenameCmdSuccessRate, filenameAppUsage) {
    getCommandSuccessRate('cmd_delay_to_pos_any', function(err, commandSuccessRates) {
        if (err) {
            console.error(err.message);
            return;
        }
        getAppUsages(commandSuccessRates, function (err, appUsages) {
            if (err) {
                console.error(err.message);
                return;
            }
            mergeArraysById(commandSuccessRates, appUsages, function (res1, res2) {
                //console.log('Cmd Success Rates\n\n');
                writeToCsvFile(res1,filenameCmdSuccessRate);
                writeToCsvFile(res2, filenameAppUsage);
                // res1.forEach(function (cmdSr) {
                //     console.log(cmdSr);
                // });
                //console.log('App openings\n\n');
                //res2.forEach(function (appU) {
                  //  console.log(appU);
                //});
            });
        });
    });
}

function writeToCsvFile(data, filename, options) {
    var headers = [];
    for (var i = 0; i < data.length; i++) {
        headers.push('val ' + i);
    }
    var writer = csvWriter({headers: headers, sendHeaders: false});
    writer.pipe(fs.createWriteStream(filename, options));
    writer.write(data);
    writer.end();
}

function mergeArraysById(arr1, arr2, cb) {
    var resultArr1 = [];
    var resultArr2 = [];

    for (var i = 0; i < arr1.length; i++) {
        for (var j = 0; j < arr2.length; j++) {
            if (arr1[i]._id.toString() === arr2[j]._id.toString()) {
                resultArr1.push(Math.round(arr1[i].cmdDelay * 1000) / 1000);
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
    var pipeline = [
        {'$match': {status: 'ACTIVE', created_at: {$lte: new Date("2017-06-01T00:00:00.000Z")}}},
        {'$group': {_id: '$user_id', countTrackers: {$sum: 1}}},
        {'$match': {countTrackers: 1}},
        {'$skip': Math.random() * 100},
        {'$limit': 5000}
    ];
    db.aggregate(connection,'ppl_subscriptions', pipeline, cb);
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

function getAppUsages(commandSuccessRates, cb) {
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
    var pipeline = [
        {
            '$match': {
                user_id: {$in: userIds},
                created_at: {
                    $gte: new Date("2017-06-01T00:00:00.000Z"),
                    $lt: new Date("2017-06-11T00:00:00.000Z")
                },
                event: 'app_foreground',
                client_id: {$in: clientIds}
            }
        },
        {'$group': {_id: '$user_id', appOpenings: {$sum: 1}}},
        {'$match': {appOpenings: {$gte: 1}}}
    ];
    db.aggregate(connection, 'mobile_events', pipeline, cb);
}

function getCommandSuccessRate(cb) {
    async.series({
        userIds: getRandomUserIds
    }, function (err, result) {
        async.waterfall([
            async.apply(db.createConnection, 'tractivedb_metrics'),
            async.apply(queryCommandSuccessRate, extractArrayContent(result.userIds))
        ], function (err, result) {
            cb(err ,result);
        });
    });
}

function queryCommandSuccessRate(userIds, connection, cb) {
    var commands = [
        "MSG_S2D_LIVE_TRACKING_MODE"
    ];
    var pipeline = [
        {
            '$match': {
                msg_name: {$in: commands},
                requested_by: {$in: userIds},
                created_at: {
                    $gte: new Date("2017-06-01T00:00:00.000Z"),
                    $lt: new Date("2017-06-11T00:00:00.000Z")
                }
            }
        },
        {'$group': {_id: '$requested_by', cmdSuccessRate: {$avg: '$cmd_success_rate'}}},
        {'$limit': 1086}
    ];

    db.aggregate(connection, 'server_command_metrics', pipeline, cb);
}

function getCommandDelayAverage(cmdDelayType) {
    find('tractivedb_metrics', queryCommandDelayAverage, cmdDelayType, function(err, result) {
        if (err) {
            console.error(err.message);
        } else {
            console.log(result);
        }
    });
}

function find(database, queryFunc, queryFuncParameter, cb) {
    async.waterfall([
        async.apply(db.createConnection, database),
        async.apply(queryFunc, queryFuncParameter)
    ], function (err, result) {
        cb(err, result);
    });
}

function queryCommandDelayAverage(cmdDelayType, connection, cb) {
    var matchCriteria = {};
    matchCriteria[cmdDelayType] = {'$exists': true};
    matchCriteria['requested_at'] = {
        '$gte': new Date("2017-06-01T00:00:00.000Z"),
        '$lt': new Date("2017-06-11T00:00:00.000Z")
    };

    var pipeline = [
        {'$match': matchCriteria},
        {'$group': {_id: null, cmdDelayAverage: {$avg: '$' + cmdDelayType}}}
    ];
    db.aggregate(connection, 'server_command_metrics', pipeline, cb);
}

function getCommandDelayForUsers(cmdDelayType, cb) {
    async.series({
        userIds: getRandomUserIds
    }, function(err, result) {
        async.waterfall([
            async.apply(db.createConnection, 'tractivedb_metrics'),
            async.apply(queryCommandDelayForUsers, cmdDelayType, extractArrayContent(result.userIds))
        ], function (err, result) {
            cb(err, result);
        });
    });
}

function queryCommandDelayForUsers(cmdDelayType, userIds, connection, cb) {
    var matchCriteria = {};
    matchCriteria[cmdDelayType] = {'$exists': true};
    matchCriteria['requested_by'] = {'$in': userIds};
    matchCriteria['created_at'] = {
        '$gte': new Date("2017-06-01T00:00:00.000Z"),
        '$lt': new Date("2017-06-11T00:00:00.000Z")
    };
    var pipeline = [
        {'$match': matchCriteria},
        {'$group': {_id: '$requested_by', cmdDelay: {$avg: '$' + cmdDelayType}}},
        {'$limit': 1086}
    ];
    db.aggregate(connection, 'server_command_metrics', pipeline, cb);
}




