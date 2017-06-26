var async = require('async');
var db = require('./db');
var util = require('./util');

module.exports.getAppUsages = getAppUsagesForUsers;

function getAppUsagesForUsers(userIds, clients, startDate, endDate, cb) {
    async.series({
        clientIds: clients
    }, function (err, result) {
        async.waterfall([
            async.apply(db.createConnection, 'tractivedb_metrics'),
            async.apply(queryAppUsagesForUsers, userIds,
                        util.getObjectIdsAsStringArray(result.clientIds, '_id'),
                        startDate, endDate)
        ], function (err, appUsages) {
            cb(err, appUsages);
        });
    });
}

function queryAppUsagesForUsers(userIds, clientIds, startDate, endDate, connection, cb) {
    var pipeline = [
        {
            '$match': {
                user_id: {$in: userIds},
                created_at: {
                    $gte: startDate,
                    $lt: endDate
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