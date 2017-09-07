var async = require('async');
var db = require('../db');

var COLLECTION = 'user_survey_answers';

module.exports = {
    getUserSurveyData: getUserSurveyData
};

function getUserSurveyData(cb) {
    async.series([
        db.createTractiveDbConnection,
        findUserSurveyData
    ], function(err, result) {
        cb(err, result[1]);
    });
}

function findUserSurveyData(cb) {
    return db.find(db.getTractiveDbConnection(), COLLECTION, {tracker_id: {$exists: true}}, {_id: 0, submit_date: 1, rating: 1, recommendation_score: 1, user_id: 1, tracker_id: 1}, {}, cb);
}