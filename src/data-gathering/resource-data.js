var async = require('async');
var util = require('../util');
var db = require('../db');
var _ = require('lodash');

var COLLECTION = 'resources';

module.exports = {
  insertResourceData: insertResourceData
};

function insertResourceData(dataInstances, cb) {
    console.log('Fetching resource data...');
    async.waterfall([
        async.apply(getResourceData, dataInstances),
        async.apply(util.mergeData, '_id', 'user_id', dataInstances)
    ], cb);
}

function getResourceData(dataInstances, cb) {
    async.map(dataInstances, getNumberOfResourcesByType, cb);
}

function getNumberOfResourcesByType(data, cb) {
    queryNumberOfResourcesByType(data.user_id, data.submit_date, function(err, result) {
        if (err) {
            return cb(err, null);
        }
        var resourcesData = {
            _id: data.user_id,
            number_of_geotraces: 0,
            number_of_images: 0
        };
        if (!!result && result.length > 0) {
            resourcesData.number_of_geotraces = findResourcesByType(result, 'geotrace');
            resourcesData.number_of_images = findResourcesByType(result, 'image');
        }
        cb(err, resourcesData);
    });
}

function findResourcesByType(resources, type) {
    var foundResource = _.find(resources, function (resource) {
        return resource._id.type.toLowerCase() === type.toLowerCase();
    });
    return !!foundResource ? foundResource.count : 0;
}

function queryNumberOfResourcesByType(userId, submitDate, cb) {
    var pipeline = [
        {
            '$match': {
                user_id: userId,
                created_at: {$lte: submitDate}
            }
        },
        {
            '$group': {_id: {user_id: '$user_id', type: '$_type'}, count: {$sum: 1}}
        }
    ];
    return db.aggregate(db.getTractiveDbConnection(), COLLECTION, pipeline, cb);
}