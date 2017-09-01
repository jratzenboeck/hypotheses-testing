var async = require('async');
var util = require('../util');

module.exports = {
    insertAverageStatisticValue: insertAverageStatisticValue
};

function insertAverageStatisticValue(dataInstances, func, cb) {
    async.waterfall([
        async.apply(func, dataInstances),
        async.apply(util.mergeData, '_id', 'tracker_id', dataInstances)
    ], cb);
}