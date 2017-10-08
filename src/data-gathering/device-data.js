var async = require('async');
var devices = require('../trackers');
var util = require('../util');

module.exports = {
    insertDeviceData: insertDeviceData
};

function insertDeviceData(dataInstances, cb) {
    console.log('Fetching device data by tracker_id...');
    async.waterfall([
        async.apply(getDeviceData, dataInstances),
        async.apply(util.mergeData, '_id', 'tracker_id', dataInstances)
    ], cb);
}

function getDeviceData(dataInstances, cb) {
    async.map(dataInstances, devices.getDeviceData, cb);
}