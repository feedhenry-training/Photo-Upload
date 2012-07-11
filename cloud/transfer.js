var fs = require('fs');
var knox = require('knox');
var async = require('async');

var client = knox.createClient({
  key: '<your-key>',
  secret: '<your-secret>',
  bucket: '<your-bucket>'
});

var imageContentType = "image/jpeg";
var imageFileExtension = ".jpg";

function getListOfUnTransferred(callback) {
  console.log('in getListOfUnTransferred with ts:' + Date.now());

  $fh.db({
    "act": "list",
    "type": "pictures",
    "fields": ["ts", "transferred"],
    "eq": {
      "transferred": false
    }
  }, function(err, data) {
    console.log('in getListOfUnTransferred with ts:' + Date.now(), ',  (fh.db callback) - data: ', data);
    return callback(null, data);
  });
};

function getPictureData(guid, cb) {
  console.log('in getPictureData with ts:' + Date.now(), ', guid: ', guid);

  $fh.db({
    "act": "read",
    "type": "pictures",
    "guid": guid
  }, function(err, data) {
    return cb(null, data);
  });

};

function updatePicture(guid, fields, callback) {
  console.log('in updatePicture with ts:' + Date.now());

  $fh.db({
    "act": "update",
    "type": "pictures",
    "guid": guid,
    fields: fields
  }, function(err, data) {
    return callback(null);
  });
};

exports.doTransfer = function(cb) {
  console.log('in doTransfer with ts:' + Date.now());
  listOfUrls = [];
  getListOfUnTransferred(function(err, dbList) {
    console.log('in doTransfer, found ' + dbList.count + ' files to send to S3');
    async.forEachSeries(
    dbList.list, function(item, itemcallback) {
      getPictureData(item.guid, function(err, data) {
        console.log('in doTransfer with ts:' + Date.now(), ', picture data - err: ', err, ', data: ', data);
        if (!data.fields.data) {
          console.log('in doTransfer: no data for picture ', item.guid, ' adding blank');
          data.fields.data = "";
        }
        if (!data.fields.ts) {
          console.log('in doTransfer: no name for picture ', item.guid, ' setting to now');
          data.fields.ts = Date.now();
        }
        var decodedImage = new Buffer(data.fields.data, 'base64');
        sendDataToS3(decodedImage, data.fields.ts, function(err, url) {
          if (!err) {
            data.fields.transferred = true;
            data.fields.url = url;
            listOfUrls.push(url);
            updatePicture(data.guid, data.fields, function(err) {
              return itemcallback(err)
            });
          } else {
            return itemcallback(err);
          }
        });
      });
    }, function(err) {
      return cb(err, listOfUrls);
    });
  });
};

function sendDataToS3(buf, nameInBucket, cb) {
  var fileName = nameInBucket.toString() + imageFileExtension;
  var req = client.put(fileName, {
    'Content-Length': buf.length,
    'Content-Type': imageContentType
    //, 'x-amz-acl': 'private'      // uncomment this line to make private
  });
  req.on('response', function(res) {
    var err = undefined;
    if (200 == res.statusCode) {
      console.log('saved to %s', req.url);
    } else {
      err = new Error("Error sending file: " + res.statusCode);
    }
    return cb(err, req.url);
  });
  req.end(buf);
};
