var csvWriter = require('csv-write-stream');
var fs = require('fs');

module.exports = {
    getObjectIdsAsStringArray: getObjectIdsAsStringArray,
    writeToCsvFile: writeToCsvFile,
    writeResultsToConsole: writeResultsToConsole,
    writeResultsToFile: writeResultsToFile,
    mergeArraysById: mergeArraysById
};

function getObjectIdsAsStringArray(jsonArray) {
    var extractedArray = [];
    jsonArray.forEach(function (jsonData) {
        extractedArray.push(jsonData._id);
    });
    return extractedArray;
}

function writeToCsvFile(data, filename, options) {
    var headers = [];
    for (var i = 0; i < data.length; i++) {
        headers.push('val ' + i);
    }
    var writer = csvWriter({headers: headers, sendHeaders: false});
    writer.pipe(fs.createWriteStream(filename, options));
    writer.write(data);
    writer.end();
}

function writeResultsToConsole(res1, res2) {
    console.log('Cmd Success Rates\n\n');
    res1.forEach(function (cmdSr) {
        console.log(cmdSr);
    });

    console.log('App openings\n\n');
    res2.forEach(function (appU) {
        console.log(appU);
    });
}

function writeResultsToFile(res1, res2, fileName1, fileName2) {
    writeToCsvFile(res1, fileName1);
    writeToCsvFile(res2, fileName2);
}

function mergeArraysById(arr1, arr2, resultFieldNameArr1, resultFieldNameArr2, cb) {
    var resultArr1 = [];
    var resultArr2 = [];

    for (var i = 0; i < arr1.length; i++) {
        for (var j = 0; j < arr2.length; j++) {
            if (arr1[i]._id.toString() === arr2[j]._id.toString()) {
                resultArr1.push(Math.round(arr1[i][resultFieldNameArr1] * 1000) / 1000);
                resultArr2.push(arr2[j][resultFieldNameArr2]);
            }
        }
    }
    cb(resultArr1, resultArr2);
}