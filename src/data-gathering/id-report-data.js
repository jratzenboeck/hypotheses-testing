var async = require('async');
var idReports = require('../dvc-id-reports');
var util = require('../util');

module.exports = {
    insertNumberOfDaysInUse: insertNumberOfDaysInUse
};

function insertNumberOfDaysInUse(dataInstances, cb) {
    console.log('Fetching number of days in use...');
    async.waterfall([
        async.apply(getNumberOfDaysInUse, dataInstances),
        async.apply(util.mergeData, '_id', 'tracker_id', dataInstances)
    ], cb);
}

function getNumberOfDaysInUse(dataInstances, cb) {
    async.map(dataInstances, idReports.getDaysInUseOfTracker, cb);
}