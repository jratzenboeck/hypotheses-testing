var db = require('../db');

var COLLECTION = 'clients';
var exceptedClients = ['amazon_echo', 'bmicalculator', 'factorytesttool', 'graph', 'petmanager', 'tms', 'notification', 'catalysts', 'hardware', 'channel', 'dashboard', 'magento', 'site'];

module.exports = {
    getClients: getClients,
    exceptedClients: exceptedClients
};

function getClients(cb) {
    var pipeline = [
        {
            '$match': {
                app_name: {$nin: exceptedClients}
            }
        },
        {
            '$group': {
                '_id': {
                    mobile_os: '$mobile_os',
                    app_name: '$app_name'
                }
            }
        }
    ];
    db.aggregate(db.getTractiveDbConnection(), COLLECTION, pipeline, cb);
}