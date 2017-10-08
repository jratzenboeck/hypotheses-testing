var async = require('async');
var util = require('../util');
var db = require('../db');
var _ = require('lodash');

var COLLECTION = 'geofences';

module.exports = {
    insertGeofenceData: insertGeofenceData
};

function insertGeofenceData(dataInstances, cb) {
    console.log('Fetching geofence data...');

    async.waterfall([
        async.apply(getGeofenceData, dataInstances),
        async.apply(util.mergeData, '_id', 'tracker_id', dataInstances)
    ], cb);
}

function getGeofenceData(dataInstances, cb) {
    async.map(dataInstances, getGeofenceDataForDevice, cb);
}

function getGeofenceDataForDevice(data, cb) {
    async.parallel([
        async.apply(aggregateGeofencesForDevice, data.tracker_id, data.submit_date),
        async.apply(queryNumberOfActiveGeofencesForDevice, data.tracker_id, data.submit_date)
        //async.apply(aggregateGeofenceIconsForDevice, data.tracker_id, data.submit_date),
    ], function(err, result) {
        if (err) {
            return cb(err, null);
        } else {
            var geofenceData = {
                _id: data.tracker_id,
                number_of_circular_geofences: 0,
                number_of_rectangular_geofences: 0,
                number_of_active_geofences: 0,
                home: 0,
                tractive: 0,
                paw: 0,
                bone: 0,
                work: 0
            };
            geofenceData._id = data.tracker_id;
            geofenceData.number_of_circular_geofences = getNumberOfGeofencesByShape(result[0], 'CIRCLE');
            geofenceData.number_of_rectangular_geofences = getNumberOfGeofencesByShape(result[0], 'RECTANGLE');
            geofenceData.number_of_active_geofences = result[1];

            cb(err, geofenceData);
        }
    });

}

function getNumberOfGeofencesByShape(geofences, shape) {
    var geofencesWithShape = _.first(_.filter(geofences, function(geoFence) {
        return !!geoFence._id && geoFence._id.shape === shape;
    }));
    return !!geofencesWithShape ? geofencesWithShape.count : 0;
}

function aggregateGeofencesForDevice(trackerId, submitDate, cb) {
    var pipeline = [
        {
            '$match': {
                device_id: trackerId,
                created_at: {$lte: submitDate}
            }
        },
        {
            '$group': {_id: {device_id: '$device_id', shape: '$shape'}, count: {'$sum': 1}}
        }
    ];
    return db.aggregate(db.getTractiveDbConnection(), COLLECTION, pipeline, cb);
}

function queryNumberOfActiveGeofencesForDevice(trackerId, submitDate, cb) {
    return db.count(db.getTractiveDbConnection(), COLLECTION, {device_id: trackerId, created_at: {$lte: submitDate}, active: true}, cb);
}

function aggregateGeofenceIconsForDevice(trackerId, submitDate, cb) {
    var pipeline = [
        {
            '$match': {
                device_id: trackerId,
                created_at: {$lte: submitDate}
            }
        },
        {
            '$group': {_id: {device_id: '$device_id', icon: '$icon'}, count: {'$sum': 1}}
        }
    ];
    return db.aggregate(db.getTractiveDbConnection(), COLLECTION, pipeline, cb);
}