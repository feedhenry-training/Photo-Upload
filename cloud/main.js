var util = require('util');
var transfer = require('transfer');

exports.transfer = function(params, callback) {
  console.log('in transfer with ts: ' + Date.now());
  transfer.doTransfer(function(err, listOfUrls) {
    console.log('doTransfer callback, err: ', err);
    console.log('doTransfer callback, listOfUrls: ', listOfUrls);

    var urls = (listOfUrls) ? listOfUrls : [];

    var ret = {
      status: "ok",
      message: "saved " + urls.length + " images",
      urls: urls
    };

    if (err) {
      ret.status = "error";
      ret.message = err.toString();
    }

    return callback(null, ret);
  });
};

exports.postPicture = function(params, callback) {
  console.log('in postPicture with ts:' + Date.now());

  $fh.db({
    "act": "create",
    "type": "pictures",
    "fields": {
      "data": params.data,
      "ts": params.ts,
      "transferred": false
    }
  }, function(err, data) {
    if (err) {
      console.log('Picture write failed');
      console.log("Error " + err);
    } else {
      console.log('Picture wrote okay!');
      console.log('scheduling transfer to S3');
      // Uncomment to transfer to S3
      // setTimeout(function() {
      //   exports.transfer(function(err, ret) {
      //     console.log('transfer finished with status: ', ret);
      //   });
      // }, 1);
      return callback(null, {
        status: "ok"
      });
    }
  });
};

exports.getList = function(params, callback) {
  console.log('in getList with ts:' + Date.now());

  $fh.db({
    "act": "list",
    "type": "pictures"
  }, function(err, data) {
    return callback(null, {
      status: "ok",
      pictures: data
    });
  });
};

exports.deletePictures = function(params, callback) {
  console.log('in deletePictures with ts:' + Date.now());

  $fh.db({
    "act": "list",
    "type": "pictures",
    "fields": ["ts", "transferred"]
  }, function(err, data) {
    var pictures = data.list;
    var picture_count = pictures.length;

    for (var i = 0; i < picture_count; i++) {
      var picture = pictures[i];
      var guid = picture.guid;

      $fh.db({
        "act": "delete",
        "type": "pictures",
        "guid": guid
      }, function(err, data) {});
    };

    return callback(null, {
      status: "ok"
    });
  });
};