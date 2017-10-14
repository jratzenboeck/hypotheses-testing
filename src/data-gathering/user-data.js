var async = require('async');
var util = require('../util');
var db = require('../db');
var _ = require('lodash');
var clients = require('./client-data');

var COLLECTION = 'users';

module.exports = {
    insertUserData: insertUserData
};

function insertUserData(dataInstances, cb) {
    console.log('Fetching user data...');

    async.waterfall([
        async.apply(getUserData, dataInstances),
        async.apply(util.mergeData, '_id', 'user_id', dataInstances)
    ], cb);
}

function getUserData(dataInstances, cb) {
    async.map(dataInstances, findUserData, cb);
}

function findUserData(data, cb) {
    var pipeline = [
        {
            '$match': {
                _id: data.user_id
            }
        },
        {
            '$lookup': {
                from: 'clients',
                localField: 'user_apps.client_id',
                foreignField: '_id',
                as: 'user_apps'
            }
        },
        {
            '$lookup': {
                from: 'tractive_start_data',
                localField: '_id',
                foreignField: 'user_id',
                as: 'tractive_start_data'
            }
        },
        {
            '$project': {
                _id: 1, email: 1, zendesk_user: 1, 'demographics.country': 1, 'demographics.language': 1,
                'user_data.gender': 1, friend_ids: 1, accounts: 1, user_apps: 1, membership_type: 1,
                tractive_start: {
                    $cond: {
                        if: { $gt: [ {$size: '$tractive_start_data'}, 0 ] },
                        then: 'yes', else: 'no'
                    }
                }
            }
        }
    ];
    db.aggregate(db.getTractiveDbConnection(), COLLECTION, pipeline, function(err, userData) {
        if (err) {
            return cb(err, null);
        }
        prepareUserResultData(userData[0], data.submit_date, function(err, result) {
            cb(err, result);
        });
    });
}

function prepareUserResultData(userData, surveySubmitDate, cb) {
    var resultData = {};
    resultData = _.assign(resultData, _.pick(userData, ['_id', 'email', 'zendesk_user', 'account_type', 'membership_type', 'tractive_start']));
    resultData.country = !!userData.demographics && !!userData.demographics.country ? userData.demographics.country : -1;
    resultData.language = !!userData.demographics && !!userData.demographics.language ? userData.demographics.language : -1;
    resultData.gender = !!userData.user_data && !!userData.user_data.gender ? userData.user_data.gender : -1;
    resultData.number_of_friends = !!userData.user_data && !!userData.user_data.friend_ids ? user_data.friend_ids.length : 0;
    resultData.number_of_social_accounts = !!userData.user_data && !!userData.accounts ? _.filter(userData.accounts, function(account) {
       return account.created_at < surveySubmitDate; // make sure that account was created before submitting the survey
    }).length : 0;
    resultData.number_of_fb_accounts = filterSocialAccounts(userData.accounts, 'facebook');
    resultData.nubmer_of_gplus_accounts = filterSocialAccounts(userData.accounts, 'gplus');
    resultData.nubmer_of_twitter_accounts = filterSocialAccounts(userData.accounts, 'twitter');
    resultData.number_of_apps = !!userData.user_apps ? _.filter(userData.user_apps, function(userApp) {
        return clients.exceptedClients.indexOf(userApp.appName) < 0 && userApp.created_at < surveySubmitDate;
    }).length : 0;
    return expandUserDataWithClientInformation(resultData, userData, cb);
}

function filterSocialAccounts(accounts, platform) {
    return _.filter(accounts, function(account) {
        return account.platform.toLowerCase() === platform;
    }).length;
}

function expandUserDataWithClientInformation(resultData, userData, cb) {
    clients.getClients(function(err, clients) {
        if (err) {
            return cb(err, null);
        }
        else {
            _.forEach(clients, function(client) {
               var clientData = client._id;
               resultData[clientData.app_name + '_' + clientData.mobile_os] =
                   _.some(userData.user_apps, {app_name: clientData.app_name, mobile_os: clientData.mobile_os}) ? 1 : 0;
            });
            return cb(err, resultData);
        }
    });
}
