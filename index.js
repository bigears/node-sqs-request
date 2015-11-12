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

  return function(req, res) {
    debug('processing request');

    function awsKey(index, name) {
      return req.uuid + '/' + index + '_' + name;
    }

    // This is the message payload. It includes the Bucket, Location (inline or
    // s3), the message id, the message body and a list of attached files.
    var payload = {
      body: req.body,
      files: Object.keys(req.files).map(function(key, index) {
        return {
          'index': index,
          'name': key,
          'key': awsKey(index, key)
        };
      }),
      Bucket: params.bucket,
      Region: params.region
    };

    var uploads = bucket.then(
      function(bucket) {
        // Find every file and upload it
        return Promise.all(Object.keys(req.files).map(
          function(name, index) {
            var file     = req.files[name]
            , fileStream = fs.createReadStream(file.path)
            , key        = awsKey(index, name)
            ;

            return bucket.putObjectAsync({
              Body: fileStream,
              Key:  key,
              ContentType: file.mimetype
            })
            .thenReturn(key);
          }
        ));
      }
    )
    .tap(debug);

    return uploads.then(function(keys) {
      return publisher.publish(payload);
    })
    .thenReturn(payload);
  };
};
