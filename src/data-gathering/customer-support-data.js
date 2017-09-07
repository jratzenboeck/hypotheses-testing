var async = require('async');
var zendesk = require('node-zendesk');
var util = require('../util');
var _ = require('lodash');

var client = zendesk.createClient({
    username:  'zendesk@tractive.com',
    token:     'hWm7gQNSBtWPeQHmkL0pOFe9WZtWpCZYKrQnCqPV',
    remoteUri: 'https://tractive.zendesk.com/api/v2'
});

module.exports = {
    insertCustomerSupportData: insertCustomerSupportData
};

function insertCustomerSupportData(dataInstances, cb) {
    async.waterfall([
        async.apply(getCustomerSupportDataForUsers, dataInstances),
        async.apply(util.mergeData, 'email', 'email', dataInstances)
    ], cb);
}

function getCustomerSupportDataForUsers(dataInstances, cb) {
    async.map(dataInstances, getCustomerSupportDataForUser, cb);
}

function getCustomerSupportDataForUser(data, cb) {
    var query = 'created>' + util.formatDate(data.created_at) + ' ' +
                'created<' + util.formatDate(data.submit_date) + ' requester:' + data.email + ' type:ticket' + ' ' +
                '-tags:no_sup';
    client.search.query(query, function(err, req, tickets) {
        if (err) {
            console.log('Error occurred when fetching zendesk data for ' + data.email + ', message: ' + err.message);
            return cb(err, null);
        }
        console.log('Fetched zendesk data for ' + data.email + '\n' + 'Ticket count: ' + tickets.length);

        var supportResultData = {
            email: data.email,
            ticket_count: tickets.length
        };
        if (tickets.length > 0) {
            getMetricsForSupportTickets(tickets, function(err, metrics) {
                // var resolvedTickets = _.filter(metrics, function (metric) {
                //     return !!metric.solved_at
                // });
                // var resolutionTime;
                // if (resolvedTickets.length > 0) {
                //     resolutionTime = _.mean(_.map(resolvedTickets, 'full_resolution_time_in_minutes'));
                // }
                supportResultData.avg_waiting_time = _.mean(_.map(metrics, 'waiting_time.business'));
                supportResultData.avg_resolution_time = _.mean(_.map(metrics, 'resolution_time.business'));
                supportResultData.reopen_count = _.sum(_.map(metrics, 'reopens'));
                supportResultData.replies = _.sum(_.map(metrics, 'replies'));
            });
        }
        return cb(null, supportResultData);
    });
}

function getMetricsForSupportTickets(tickets, cb) {
    return async.map(tickets, getMetricsForSupportTicket, cb);
}

function getMetricsForSupportTicket(ticket, cb) {
    client.ticketmetrics.list(ticket.id, function(err, req, result) {
        return cb(err, {
            waiting_time: result.requester_wait_time_in_minutes,
            resolution_time: result.full_resolution_time_in_minutes,
            reopens: result.reopens,
            replies: result.replies
        });
    });
}


