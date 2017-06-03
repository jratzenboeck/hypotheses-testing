var MongoClient = require('mongodb').MongoClient;

module.exports.createConnection = createConnection;

function createConnection(database, cb) {
    MongoClient.connect('mongodb://localhost:47018/' + database, function(err, db) {
        if (err) {
            throw err;
        }
        console.log('connected to db');
        cb(null, db);
    });
}