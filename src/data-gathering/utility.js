var async = require('async');
var util = require('../util');

module.exports = {
    insertAverageStatisticValueTracker: insertAverageStatisticValueTracker,
    insertAverageStatisticValueUser: insertAverageStatisticValueUser
};

function insertAverageStatisticValueTracker(dataInstances, func, cb) {
    insertAverageStatisticValue(dataInstances, func, '_id', 'tracker_id', cb);
}

function insertAverageStatisticValueUser(dataInstances, func, cb) {
    insertAverageStatisticValue(dataInstances, func, '_id', 'user_id', cb);
}

function insertAverageStatisticValue(dataInstances, func, srcAttr, destAttr, cb) {
    async.waterfall([
        async.apply(getData, dataInstances, func),
        async.apply(util.mergeData, srcAttr, destAttr, dataInstances)
    ], cb);
}

function getData(dataInstances, func, cb) {
    async.map(dataInstances, func, cb);
}
