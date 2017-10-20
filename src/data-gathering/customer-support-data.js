var async = require('async');
var zendesk = require('node-zendesk');
var util = require('../util');
var _ = require('lodash');
var sleep = require('sleep');
var request = require('request');
var base64 = require('base-64');

var client = zendesk.createClient({
    username: 'zendesk@tractive.com',
    token: 'hWm7gQNSBtWPeQHmkL0pOFe9WZtWpCZYKrQnCqPV',
    remoteUri: 'https://tractive.zendesk.com/api/v2'
});

module.exports = {
    insertCustomerSupportData: insertCustomerSupportData
};

function buildZendeskRequest(path, query) {
    var auth_data = base64.encode(client.username + '/token:' + client.token);
    var headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'Basic emVuZGVza0B0cmFjdGl2ZS5jb20vdG9rZW46aFdtN2dRTlNCdFdQZVFIbWtMMHBPRmU5V1p0V3BDWllLclFuQ3FQVg=='
    };

    return {
        url: 'https://tractive.zendesk.com/api/v2/' + path,
        method: 'GET',
        headers: headers,
        qs: {'query': query}
    };
}

function insertCustomerSupportData(dataInstances, cb) {
    var zendeskUsers = getZendeskUsers(dataInstances);
    var nonZendeskUsers = getNonZendeskUsers(dataInstances);

    async.waterfall([
        async.apply(getCustomerSupportDataForUsers, zendeskUsers, nonZendeskUsers),
        async.apply(util.mergeData, 'email', 'email', dataInstances)
    ], cb);
}

function getZendeskUsers(dataInstances) {
    return _.filter(dataInstances, function (instance) {
        return !!instance.zendesk_user;
    });
}

function getNonZendeskUsers(dataInstances) {
    return _.reject(dataInstances, function (instance) {
        return !!instance.zendesk_user;
    });
}

function getCustomerSupportDataForUsers(zendeskUsers, nonZendeskUsers, cb) {
    async.mapSeries(zendeskUsers, getCustomerSupportDataForUser, function (err, usersWithSupportData) {
        var result = null;
        if (!err) {
            var nonZendeskUsersWithSupportData = _.map(nonZendeskUsers, function(nonZendeskUser) {
                return initSupportResultData(nonZendeskUser.email);
            });
            result = mergeUsers(nonZendeskUsersWithSupportData, usersWithSupportData);
        }
        return cb(err, result);
    });
}

function mergeUsers(zendeskUsers, nonZendeskUsers) {
    return _.union(zendeskUsers, nonZendeskUsers);
}

function initSupportResultData(email) {
    return {
        email: email,
        ticket_count: 0,
        avg_waiting_time: -1,
        avg_resolution_time: -1,
        reopen_count: -1,
        replies: -1,
        sadness_count: -1,
        joy_count: -1
    };
}
function getCustomerSupportDataForUser(data, cb) {
    sleep.sleep(2);
    var query = 'requester:' + data.email + ' type:ticket' + ' ' +
        '-tags:no_sup';

    request(buildZendeskRequest('search.json', query), function (err, response, body) {
        var supportResultData = initSupportResultData(data.email);
        if (err) {
            console.log('Error occurred when fetching zendesk data for ' + data.email + ', message: ' + err.message + ', full error: ' + err);
            return cb(null, supportResultData);
        }
        var tickets = JSON.parse(body).results;
        if (tickets.length > 0) {
            supportResultData.ticket_count = tickets.length;
            console.log('Fetching zendesk data for ' + data.email + ', ticket count: ' + supportResultData.ticket_count);
            var tagsOfTicket = _.map(tickets, 'tags');
            supportResultData.sadness_count = countTermOccurrencesInArray(tagsOfTicket, ['avg', 'sadness']);
            supportResultData.joy_count = countTermOccurrencesInArray(tagsOfTicket, ['avg', 'joy']);

            getMetricsForSupportTickets(tickets, function (err, metrics) {
                if (!err) {
                    supportResultData.avg_waiting_time = _.round(_.mean(_.map(metrics, 'waiting_time.business')), 3);
                    supportResultData.avg_resolution_time = _.round(_.mean(_.map(metrics, 'resolution_time.business')), 3);
                    supportResultData.reopen_count = _.sum(_.map(metrics, 'reopens'));
                    supportResultData.replies = _.sum(_.map(metrics, 'replies'));
                }
                return cb(null, supportResultData); // do not throw error callback here to ensure that processing tickets continues
            });
        } else {
            return cb(null, supportResultData);
        }
    });
}

function countTermOccurrencesInArray(inputArray, terms) {
    return _.filter(inputArray, function (val) {
        return val.includes(terms);
    }).length;
}

function getMetricsForSupportTickets(tickets, cb) {
    return async.map(tickets, getMetricsForSupportTicket, cb);
}

function getMetricsForSupportTicket(ticket, cb) {
    client.ticketmetrics.list(ticket.id, function (err, req, result) {
        if (err || !result) {
            console.log('Error occurred when fetching ticket metrics for ticker ' + ticket.id);
            return cb(err, null);
        }
        return cb(null, {
            waiting_time: result.requester_wait_time_in_minutes,
            resolution_time: result.full_resolution_time_in_minutes,
            reopens: result.reopens,
            replies: result.replies
        });
    });
}


