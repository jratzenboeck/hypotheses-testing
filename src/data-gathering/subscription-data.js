var async = require('async');
var subscriptions = require('../subscriptions');
var util = require('../util');

module.exports = {
    insertStartDate: insertStartDate
};

function insertStartDate(dataInstances, cb) {
    async.waterfall([
        async.apply(getStartDate, dataInstances),
        async.apply(util.mergeData, 'user_id', 'user_id', dataInstances)
    ], cb);
}

function getStartDate(dataInstances, cb) {
    async.map(dataInstances, subscriptions.getSubscriptionActivationTime, cb);
}