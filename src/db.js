var MongoClient = require('mongodb').MongoClient;

var connection = {};

module.exports = {
    createConnection: createConnection,
    createTractiveDbConnection: createTractiveDbConnection,
    createMetricsConnection: createMetricsConnection,
    aggregate: aggregate,
    find: find,
    findOne: findOne,
    getTractiveDbConnection: getTractiveDbConnection,
    getMetricsDbConnection: getMetricsDbConnection
};

function createDbConnection(database, cb) {
    MongoClient.connect('mongodb://localhost:47018/' + database, function (err, db) {
        if (err) {
            throw err;
        }
        if (database === 'tractivedb') {
            connection.tractivedb = db;
        } else {
            connection.metrics = db;
        }

        cb(null);
    });
}

function createMetricsConnection(cb) {
    createDbConnection('tractivedb_metrics', cb);
}

function createTractiveDbConnection(cb) {
    createDbConnection('tractivedb', cb);
}

function createConnection(database, cb) {
    MongoClient.connect('mongodb://localhost:47018/' + database, function (err, db) {
        if (err) {
            throw err;
        }
        connection[database] = db;

        cb(null, db);
    });
}

function getTractiveDbConnection() {
    return connection.tractivedb;
}

function getMetricsDbConnection() {
    return connection.metrics;
}

function find(connection, collection, criteria, projection, options, cb) {
    connection.collection(collection)
        .find(criteria, projection, options || {})
        .toArray(
            function (err, result) {
                //connection.close();
                cb(err, result);
            });
}

function count(connection, collection, criteria, cb) {
    connection.collection(collection)
        .count(criteria, function(err, result) {
            connection.close();
            cb(err, result);
        });
}

function findOne(connection, collection, criteria, projection, cb) {
    connection.collection(collection)
        .findOne(criteria, projection, function (err, result) {
            //connection.close();
            cb(err, result);
        });
}

function aggregate(connection, collection, pipeline, cb) {
    connection.collection(collection).aggregate(pipeline, function (err, result) {
        //connection.close();
        cb(err, result);
    });
}