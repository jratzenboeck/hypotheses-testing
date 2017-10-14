var async = require('async');
var db = require('../db');

var COLLECTION = 'user_survey_answers';

module.exports = {
    getUserSurveyDataAll: getUserSurveyDataAll,
    getUserSurveyDataUnique: getUserSurveyDataUnique
};

function getUserSurveyData(func, cb) {
    async.series([
        db.createTractiveDbConnection,
        func
    ], function(err, result) {
        cb(err, result[1]);
    });
}

function getUserSurveyDataUnique(cb) {
    getUserSurveyData(findUserSurveyDataUnique, cb);
}

function getUserSurveyDataAll(cb) {
    getUserSurveyData(findUserSurveyData, cb);
}

function findUserSurveyData(cb) {
    return db.find(db.getTractiveDbConnection(), COLLECTION, {tracker_id: {$exists: true}},
        {_id: 0, submit_date: 1, rating: 1, recommendation_score: 1, user_id: 1, tracker_id: 1}, {}, cb);
}

function findUserSurveyDataUnique(cb) {
    var pipeline = [
        {
            '$match': {
                tracker_id: {$exists: true}
            }
        },
        {
            '$group': {
                _id: '$user_id', count: {$sum: 1},
                survey_data: {
                    '$push': {
                        submit_date: '$submit_date', rating: '$rating',
                        recommendation_score: '$recommendation_score',
                        user_id: '$user_id', tracker_id: '$tracker_id'
                    }
                }
            }
        },
        {
            '$match': {
                count: 1
            }
        },
        {
            '$unwind': '$survey_data'
        },
        {
            '$project': {
                _id: 0, submit_date: '$survey_data.submit_date', rating: '$survey_data.rating',
                recommendation_score: '$survey_data.recommendation_score', user_id: '$survey_data.user_id',
                tracker_id: '$survey_data.tracker_id'
            }
        }
    ];
    return db.aggregate(db.getTractiveDbConnection(), COLLECTION, pipeline, cb);
}