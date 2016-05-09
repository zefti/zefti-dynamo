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
  var ready = 'false';
  var pendingCommands = [];
  var tableIndexes = {};
  var searchIndexMap = {};
  var dynamoVirtualClient = {};
  var dataSource = options.dataSource;
  AWS.config.update({region: 'us-east-1'});
  AWS.config.update({accessKeyId: dataSource.accessKeyId, secretAccessKey: dataSource.secretAccessKey});
  var dynamodb = new AWS.DynamoDB();

  dynamoCommands.forEach(function(command) {
    dynamoVirtualClient[command] = function () {
      var args = Array.prototype.slice.call(arguments);
      var cb = args.pop();
      var outerCb = function (err, result) {
        if (err) return cb({errCode: '56f8dbcf3e56b04f0875ceae', err: err});
        return cb(err, result);
      };
      args.push(outerCb);
      if (ready) {
        if (command === 'query' && args[0].IndexVariables) {
          var indexName = getIndexName(args[0].IndexVariables, tableIndexes, searchIndexMap);
          if (indexName === 'unknown') return cb({errCode: '56f8dbcf3e56b04f0875ceaf'});
          args[0].IndexName = indexName;
          delete args[0].IndexVariables;
        }
        dynamodb[command].apply(dynamodb, args);
      } else {
        pendingCommands.push({cmd:command, args:args});
      }
    }
  });

  dynamodb.describeTable({TableName:dataSource.table}, function(err, result){
    if (err) throw new Error('could not get global secondary indexes');
    result.Table.GlobalSecondaryIndexes.forEach(function(index){
      tableIndexes[index.IndexName] = 1;
    });
    ready = true;
    pendingCommands.forEach(function(request){
      //console.log('INSIDE (PENDING)')
      if (request.cmd === 'query' && request.args[0].IndexVariables) {
        var indexName = getIndexName(args[0].IndexVariables, tableIndexes, searchIndexMap);
        if (indexName === 'unknown') return cb({errCode: '56f8dbcf3e56b04f0875ceaf'});
        args[0].IndexName = indexName;
        delete args[0].IndexVariables;
      }
      dynamodb[request.cmd].apply(dynamodb, request.args);
    });
  });
  return dynamoVirtualClient;
};

function getIndexName(hash, tableIndexes, searchIndexMap){
  var indexName = '';
  hash.equality.sort();
  hash.equality.forEach(function(name){
    indexName = indexName + name + '-';
  });
  if (hash.range) indexName = indexName + hash.range + '-';
  indexName = indexName + 'index';
  if (searchIndexMap[indexName]) {
    return searchIndexMap[indexName];
  }
  if (tableIndexes[indexName]) {
    searchIndexMap[indexName] = indexName;
    return indexName;
  } else {
    var indexNameSegment = indexName.split('-')[0];
    for (var key in tableIndexes) {
      var indexSegment = key.split('-')[0];
      if (indexNameSegment === indexSegment) {
        searchIndexMap[indexName] = key;
        return key;
      }
    }
    return 'unknown';
  }
}