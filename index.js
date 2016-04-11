var AWS = require('aws-sdk');

var dynamoCommands = [
    'batchGetItem'
  , 'batchWriteItem'
  , 'createTable'
  , 'deleteItem'
  , 'describeLimits'
  , 'describeTable'
  , 'getItem'
  , 'listTables'
  , 'putItem'
  , 'query'
  , 'scan'
  , 'updateItem'
  , 'updateTable'
  , 'waitFor'
];


module.exports = function(options){
  var dynamoVirtualClient = {};
  var dataSource = options.dataSource;
  AWS.config.update({region: 'us-east-1'});
  AWS.config.update({accessKeyId: dataSource.accessKeyId, secretAccessKey: dataSource.secretAccessKey});
  var dynamodb = new AWS.DynamoDB();

  dynamoCommands.forEach(function(command) {
    dynamoVirtualClient[command] = function () {
      var args = Array.prototype.slice.call(arguments);
      var cb = args.pop();
      var outerCb = function(err, result){
        if (err) return cb({errCode:'56f8dbcf3e56b04f0875ceae', err:err});
        return cb(err, result);
      };
      args.push(outerCb);
      dynamodb[command].apply(dynamodb, args);
    }
  });

  return dynamoVirtualClient;
};