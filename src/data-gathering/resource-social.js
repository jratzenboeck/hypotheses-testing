var async = require('async');
var util = require('../util');
var db = require('../db');
var _ = require('lodash');

var COLLECTION_COMMENTS = 'resource_comments';
var COLLECTION_LIKES = 'resource_likes';

module.exports = {
    insertResourceCommentsData: insertResourceCommentsData,
    insertResourceLikesData: insertResourceLikesData
};

function insertResourceCommentsData(dataInstances, cb) {
    console.log('Fetching resource comments data...');
    insertResourceSocialData(dataInstances, getResourceCommentsData, cb);
}

function insertResourceLikesData(dataInstances, cb) {
    console.log('Fetching resource likes data...');
    insertResourceSocialData(dataInstances, getResourceLikesData, cb);
}

function insertResourceSocialData(dataInstances, resourceFunction, cb) {
    async.waterfall([
        async.apply(resourceFunction, dataInstances),
        async.apply(util.mergeData, '_id', 'user_id', dataInstances)
    ], cb);
}

function getResourceCommentsData(dataInstances, cb) {
    async.map(dataInstances, getNumberOfResourceCommentsForUser, cb);
}

function getResourceLikesData(dataInstances, cb) {
    async.map(dataInstances, getNumberOfResourceLikesForUser, cb);
}

function getNumberOfResourceCommentsForUser(data, cb) {
    return queryNumberOfResourceSocial(COLLECTION_COMMENTS, data.user_id, data.submit_date, function(err, result) {
        if (err) {
            return cb(err, null);
        }
        var resourceCommentsData = {_id: data.user_id, number_of_resource_comments: 0};
        if (!!result && result.length > 0) {
            resourceCommentsData.number_of_resource_comments = result[0].count;
        }
        return cb(err, resourceCommentsData);
    });
}

function getNumberOfResourceLikesForUser(data, cb) {
    return queryNumberOfResourceSocial(COLLECTION_LIKES, data.user_id, data.submit_date, function(err, result) {
        if (err) {
            return cb(err, null);
        }
        var resourceLikesData = {_id: data.user_id, number_of_resource_likes: 0};
        if (!!result && result.length > 0) {
            resourceLikesData.number_of_resource_likes = result[0].count;
        }
        return cb(err, resourceLikesData);
    });
}

function queryNumberOfResourceSocial(collection, userId, submitDate, cb) {
    var pipeline = [
        {
            '$match': {
                user_id: userId,
                created_at: {$lte: submitDate}
            }
        },
        {
            '$group': {_id: '$user_id', count: {'$sum': 1}}
        }
    ];
    return db.aggregate(db.getTractiveDbConnection(), collection, pipeline, cb);
}