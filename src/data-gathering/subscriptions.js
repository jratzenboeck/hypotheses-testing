var async = require('async');
var db = require('../db');

module.exports = {
    getSubscriptionActivationTime: getSubscriptionActivationTime
};

function getSubscriptionActivationTime(data, cb) {
    async.waterfall([
        async.apply(db.createConnection, 'tractivedb'),
        async.apply(findSubscriptionActivationTime, data.user_id, data.tracker_id)
    ], function(err, result) {
        return cb(err, result);
    });
}

function findSubscriptionActivationTime(userId, trackerId, connection, cb) {
    return db.findOne(connection, 'ppl_subscriptions',
        {
            user_id: userId,
            tracker_id: trackerId
        },
        {user_id: 1, tracker_id: 1, created_at: 1, _id: 0}, cb);
}
