var csvWriter = require('csv-write-stream');
var fs = require('fs');
var _ = require('lodash');

module.exports = {
    getObjectIdsAsStringArray: getObjectIdsAsStringArray,
    mergeAndPrint: mergeAndPrint,
    buildFilename: buildFilename,
    mergeData: mergeData,
    writeDataToFile: writeDataToFile
};

function getObjectIdsAsStringArray(jsonArray, idField) {
    var extractedArray = [];
    jsonArray.forEach(function (jsonData) {
        var data;
        if (!!idField) {
            data = jsonData['_id'][idField];
        } else {
            data = jsonData['_id'];
        }
        extractedArray.push(data)
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

function writeDataToFile(data, headers, filename) {
    var file = fs.createWriteStream(filename);

    file.on('error', function(err) {
        console.error('Error occurred during writing ' + err.message);
    });

    writeHeaders(headers, file);
    writeData(data, file);

    file.end();
}

function writeHeaders(headers, file) {
    file.write(_.join(headers, ';'));
    file.write('\n');
}

function writeData(data, file) {
    for (var i = 0; i < data.length; i++) {
        data[i] = _.omit(data[i], ['submit_date', 'created_at']);
        var dataRow = _.map(data[i], _.identity);
        file.write(_.join(dataRow, ';'));
        file.write('\n');
    }
}

function writeResultsToConsole(res1, res2, labelResult1, labelResult2) {
    console.log(labelResult1 + '\n\n');
    res1.forEach(function (entry) {
        console.log(entry);
    });

    console.log(labelResult2 + '\n\n');
    res2.forEach(function (entry) {
        console.log(entry);
    });
}

function writeResultsToFile(res1, res2, fileName1, fileName2) {
    writeDataToFile(res1, fileName1);
    writeDataToFile(res2, fileName2);
}

function mergeArraysById(arr1, arr2, resultFieldNameArr1, resultFieldNameArr2, cb) {
    var resultArr1 = [];
    var resultArr2 = [];

    for (var i = 0; i < arr1.length; i++) {
        for (var j = 0; j < arr2.length; j++) {
            if (arr1[i]['_id']['user_id'].toString() === arr2[j]['_id'].toString()) {
                resultArr1.push(Math.round(arr1[i][resultFieldNameArr1] * 1000) / 1000);
                resultArr2.push(arr2[j][resultFieldNameArr2]);
            }
        }
    }
    cb(resultArr1, resultArr2);
}

function mergeAndPrint(arr1, arr2, resultFieldNameArr1, resultFieldNameArr2, labelResult1, labelResult2, filenames) {
    mergeArraysById(arr1, arr2, resultFieldNameArr1, resultFieldNameArr2,
        function (res1, res2) {
            if (!!filenames) {
                writeResultsToFile(res1, res2, filenames[0], filenames[1]);
            } else {
                writeResultsToConsole(res1, res2, labelResult1, labelResult2);
            }
        });
}

function mergeData(mergeAttributeSrc, mergeAttributeDest, destinationData, sourceData, cb) {
    for (var i = 0; i < destinationData.length; i++) {
        for (var j = 0; j < sourceData.length; j++) {
            if (!sourceData[j]) {
                continue;
            }
            if (sourceData[j][mergeAttributeSrc].toString() === destinationData[i][mergeAttributeDest].toString()) {
                destinationData[i] = _.assign(destinationData[i], sourceData[j]);
            }
        }
    }
    cb(null, destinationData);
}

function toDateString(date) {
    return date.getFullYear() + '_' + (date.getMonth() + 1) + '_' +
        date.getUTCDate() + '_' + date.getHours() + '_' +
        date.getMinutes() + '_' + date.getSeconds();
}

function buildFilename(prefix, date, fileExtension) {
    return prefix + '_' + toDateString(date) + fileExtension;
}