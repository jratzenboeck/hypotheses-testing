var MongoClient = require('mongodb').MongoClient;

module.exports.createConnection = createConnection;
module.exports.aggregate = aggregate;

function createConnection(database, cb) {
    MongoClient.connect('mongodb://localhost:47018/' + database, function(err, db) {
        if (err) {
            throw err;
        }
        cb(null, db);
    });
}

function aggregate(connection, collection, pipeline, cb) {
    connection.collection(collection).aggregate(pipeline, function (err, result) {
        connection.close();
        cb(err, result);
    });
}