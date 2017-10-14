var async = require('async');
var util = require('../util');
var db = require('../db');
var _ = require('lodash');
var utility = require('./utility');

var COLLECTION = 'shares';

module.exports = {
    insertSentSharesForTrackers: insertShareDataForTrackers,
    insertReceivedSharesForUsers: insertReceivedSharesForUsers
};

function insertShareDataForTrackers(dataInstances, cb) {
    console.log('Fetching sent share data for trackers...');
    utility.insertAverageStatisticValueTracker(dataInstances, getNumberOfSharesForTracker, cb);
}

function insertReceivedSharesForUsers(dataInstances, cb) {
    console.log('Fetching received share data for users...');
    utility.insertAverageStatisticValueUser(dataInstances, getNumberOfReceivedSharesForUser, cb)
}

function getNumberOfReceivedSharesForUser(data, cb) {
    queryNumberOfReceivedSharesForUser(data.user_id, data.submit_date, function(err, result) {
        cb(err, {_id: data.user_id, number_of_received_shares: result});
    });
}

function queryNumberOfReceivedSharesForUser(userId, submitDate, cb) {
    return db.count(db.getTractiveDbConnection(), COLLECTION, {invitee_id: userId, created_at: {$lte: submitDate}}, cb);
}

function getNumberOfSharesForTracker(data, cb) {
    queryNumberOfSharesForTracker(data.tracker_id, data.submit_date, function(err, result) {
        cb(err, {_id: data.tracker_id, number_of_shares: result});
    });
}

function queryNumberOfSharesForTracker(trackerId, submitDate, cb) {
    return db.count(db.getTractiveDbConnection(), COLLECTION, {tracker_id: trackerId, created_at: {$lte: submitDate}}, cb);
}