var async = require('async');
var util = require('../util');
var db = require('../db');
var _ = require('lodash');
var utility = require('./utility');

var COLLECTION_ANSWERS = 'pet_survey_answers';
var COLLECTION_QUESTIONS = 'pet_survey_questions';

module.exports = {
    insertPetSurveyData: insertPetSurveyData
};

function insertPetSurveyData(dataInstances, cb) {
    console.log('Fetching pet survey data for users...');
    utility.insertAverageStatisticValueNew(dataInstances, getPetSurveyDataForUser, '_id', 'user_id', cb);
}

function getPetSurveyDataForUser(data, cb) {
    initPetQuestionHashMap(function(err, questionHashMap) {
        if (err) {
            cb(err, null);
        }
        async.waterfall([
            async.apply(initPetSurveyData, data.user_id),
            async.apply(queryPetSurveyAnswersForUser, data.user_id, data.submit_date)
        ], cb);
    })
}

function initPetSurveyData(userId, cb) {
    queryPetSurveyQuestions(function(err, questions) {
        if (err) {
            return cb(err, questions);
        }
        var petSurveyData = {_id: userId};
        _.forEach(questions, function(question) {
            petSurveyData[question._id] = -1;
        });
        return cb(err, petSurveyData);
    })
}

function queryPetSurveyQuestions(cb) {
    return db.find(db.getTractiveDbConnection(), COLLECTION_QUESTIONS, {}, {_id: 1, text: 1}, {}, cb);
}

function initPetQuestionHashMap(cb) {
    var hashMap = [];

    queryPetSurveyQuestions(function(err, questions) {
        if (err) {
            return cb(err, questions);
        }
        _.forEach(questions, function(question) {
            hashMap.push({_id: question._id.toString(), text: question.text.en});
        });
        cb(err, hashMap);
    });
}

function queryPetSurveyAnswersForUser(userId, submitDate, petSurveyData, cb) {
    return db.find(db.getTractiveDbConnection(), COLLECTION_ANSWERS, {user_id: userId, created_at: {$lte: submitDate}}, {}, {}, function(err, answers) {
        if (err) {
            return cb(err, answers);
        }
        _.forEach(answers, function(answer) {
            petSurveyData[answer.question_id.toString()] = answer.given_answer_id.toString();
        });
        cb(err, petSurveyData);
    });
}