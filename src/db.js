var MongoClient = require('mongodb').MongoClient;

module.exports = {
    createConnection: createConnection,
    aggregate: aggregate,
    find: find
};


function createConnection(database, cb) {
    MongoClient.connect('mongodb://localhost:47018/' + database, function(err, db) {
        if (err) {
            throw err;
        }
        cb(null, db);
    });
}

function find(connection, collection, criteria, projection, cb) {
    connection.collection(collection)
        .find(criteria, projection)
        .toArray(
            function (err, result) {
                connection.close();
                cb(err, result);
            });
}
function aggregate(connection, collection, pipeline, cb) {
    connection.collection(collection).aggregate(pipeline, function (err, result) {
        connection.close();
        cb(err, result);
    });
}