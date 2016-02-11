var AWS = require('aws-sdk');


module.exports = function(options){
  var dataSource = options.dataSource;
  AWS.config.update({region: 'us-east-1'});
  AWS.config.update({accessKeyId: dataSource.accessKeyId, secretAccessKey: dataSource.secretAccessKey});
  var dynamodb = new AWS.DynamoDB();
  return dynamodb;
};