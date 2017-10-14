var async = require('async');
var util = require('../util');

module.exports = {
    insertAverageStatisticValue: insertAverageStatisticValue,
    insertAverageStatisticValueUser: insertAverageStatisticValueUser,
    insertAverageStatisticValueNew: insertAverageStatisticValueNew
};

function insertAverageStatisticValue(dataInstances, func, cb) {
    async.waterfall([
        async.apply(func, dataInstances),
        async.apply(util.mergeData, '_id', 'tracker_id', dataInstances)
    ], cb);
}

function insertAverageStatisticValueUser(dataInstances, func, cb) {
    async.waterfall([
        async.apply(func, dataInstances),
        async.apply(util.mergeData, '_id', 'user_id', dataInstances)
    ], cb);
}

function insertAverageStatisticValueNew(dataInstances, func, srcAttr, destAttr, cb) {
    async.waterfall([
        async.apply(getData, dataInstances, func),
        async.apply(util.mergeData, srcAttr, destAttr, dataInstances)
    ], cb);
}

function getData(dataInstances, func, cb) {
    async.map(dataInstances, func, cb);
}
