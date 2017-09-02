var async = require('async');
var db = require('./db');

module.exports = {
    getSubscriptionActivationTime: getSubscriptionActivationTime
};

function getSubscriptionActivationTime(data, cb) {
    findSubscriptionActivationTime(data.user_id, data.tracker_id, cb);
}

function findSubscriptionActivationTime(userId, trackerId, cb) {
    return db.findOne(db.getTractiveDbConnection(), 'ppl_subscriptions',
        {
            user_id: userId,
            tracker_id: trackerId
        },
        {user_id: 1, tracker_id: 1, created_at: 1, _id: 0}, cb);
}
