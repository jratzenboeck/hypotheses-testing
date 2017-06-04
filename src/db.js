var MongoClient = require('mongodb').MongoClient;

module.exports.createConnection = createConnection;

function createConnection(database, cb) {
    MongoClient.connect('mongodb://localhost:47018/' + database, function(err, db) {
        if (err) {
            throw err;
        }
        cb(null, db);
    });
}