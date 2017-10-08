var async = require('async');
var subscriptions = require('../subscriptions');
var util = require('../util');

module.exports = {
    insertSubscriptionData: insertSubscriptionData
};

function insertSubscriptionData(dataInstances, cb) {
    console.log('Fetching subscriptions data...');
    async.waterfall([
        async.apply(getSubscriptionData, dataInstances),
        async.apply(util.mergeData, 'user_id', 'user_id', dataInstances)
    ], cb);
}

function getSubscriptionData(dataInstances, cb) {
    async.map(dataInstances, subscriptions.getSubscriptionDataForUser, cb);
}