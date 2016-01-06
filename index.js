/*
* Create a function that can accept a request and puts the request into an SQS
* queue, and any attached files on to S3.
*
* @example
*   var sqsRequest = require('sqs-request')({
*     region: 'eu-west-1',
*     bucket: 'files',
*     queue: 'requestQueue'
*   });
*
*   app.use('/', sqsRequest);
*
* The request must have a uuid.
*/

var fs           = require('fs');
var Promise      = require('bluebird');
var AWS          = require('aws-sdk');
var regionBucket = require('region-bucket');
var Publisher    = require('bigears-sqs-publisher');
var debug        = require('debug')('sqs-request');

/*
* Find the bucket. If the bucket is found then restablish s3 connection to the
* correct region. Otherwise create the bucket.
*/
function createBucket(region, bucket) {
  return regionBucket(region, bucket, {
    create: true,
    ACL: 'private'
  });
}

module.exports = function(params) {
  var bucket = createBucket(params.region, params.bucket);
  var publisher = new Publisher(params.region, params.queue);

  return function(req, res, files) {
    debug('processing request with body', req.body);

    function awsKey(index, name) {
      return req.uuid + '/' + index + '_' + name;
    }

    // This is the message payload. It includes the Bucket, Location (inline or
    // s3), the message id, the message body and a list of attached files.
    var payload = {
      body: req.body,
      files: files.map(function(file, index) {
        return {
          'index': index,
          'name': file.fieldname,
          'key': awsKey(index, file.fieldname)
        };
      }),
      Bucket: params.bucket,
      Region: params.region
    };

    debug('processing request with', files.length, 'files attached');

    var uploads = bucket.then(
      function(bucket) {
        // Find every file and upload it
        return Promise.all(files.map(
          function(file, index) {
            var fileStream = fs.createReadStream(file.path);
            var key = awsKey(index, file.fieldname);

            debug('uploading file', key);

            return bucket.putObjectAsync({
              Body: fileStream,
              Key:  key,
              ContentType: file.mimetype
            })
            .then(function(res) {
              debug('uploaded file', key);
              return res;
            })
            .then(function() {
              return key;
            });
          }
        ));
      }
    );

    return uploads.then(function(keys) {
      debug('uploaded', keys.length, 'files');
      return publisher.publish(payload);
    })
    .then(function() {
      return payload;
    });
  };
};
