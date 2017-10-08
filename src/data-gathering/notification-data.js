var async = require('async');
var util = require('../util');
var db = require('../db');
var _ = require('lodash');

var COLLECTION = 'notifications';

module.exports = {
    insertNotificationsData: insertNotificationsData
};

function insertNotificationsData(dataInstances, cb) {
    console.log('Fetching notifications data...');

    async.waterfall([
        async.apply(getNotificationsData, dataInstances),
        async.apply(util.mergeData, '_id', 'user_id', dataInstances)
    ], cb);
}

function getNotificationsData(dataInstances, cb) {
    async.map(dataInstances, getNumberOfNotificationsByType, cb);
}

function getNumberOfNotificationsByType(data, cb) {
    async.waterfall([
        async.apply(queryNumberOfNotificationsByType, data.user_id, data.submit_date),
        async.apply(buildNotificationsData, data.user_id)
    ], cb);
}

function buildNotificationsData(userId, notifications, cb) {
    var notificationsData = {_id: userId};

    queryNotificationTypes(function (err, types) {
        if (err) {
            return cb(err, null);
        }
        _.forEach(types, function (type) {
            notificationsData[_.snakeCase(type)] = findNotificationCountByType(notifications, type)
        });
        cb(err, notificationsData);
    });
}

function findNotificationCountByType(notifications, type) {
    var foundNotification = _.find(notifications, function (notification) {
        return notification._id.type === type;
    });
    return !!foundNotification ? foundNotification.count : 0;
}

function queryNumberOfNotificationsByType(userId, submitDate, cb) {
    var pipeline = [
        {
            '$match': {
                user_id: userId,
                created_at: {$lte: submitDate}
            }
        },
        {
            '$group': {_id: {user_id: '$user_id', type: '$_type'}, count: {'$sum': 1}}
        }
    ];
    return db.aggregate(db.getTractiveDbConnection(), COLLECTION, pipeline, cb);
}

function queryNotificationTypes(cb) {
    return db.distinct(db.getTractiveDbConnection(), COLLECTION, '_type', cb);
}