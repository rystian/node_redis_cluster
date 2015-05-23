var client = require('../index').clusterClient;
var Promise = require('bluebird');

client.clusterInstance('127.0.0.1:7000', function (err, conn) {
  if (err) {
    console.log('error when connecting to cluster : ' + err)
    throw err;
  }
  //What's with the curly brackets?
  //see : http://redis.io/topics/cluster-tutorial#Redis Cluster data sharding
  var key = '{myfoo}';

  try {
    //rdClusterNode.on('ready',function(){
    var pclient = Promise.promisifyAll(conn);

    //test promisified hset
    pclient.hsetAsync('foo', 'bar', 'baz').then(function () {
      console.log('finished processing hsetAsync...');
      var pclientMulti = Promise.promisifyAll(pclient.multi(key));
      var data = [];
      for (var j = 0; j < 5; j++) {
        for (var i = 0; i < 5; i++) {
          var fieldName = 'hmset' + i;
          data[fieldName] = 'value-' + i;

        }
        var _key = key + ':' + j;
        pclientMulti.hmsetAsync(_key, data).then(function (res) {
          console.log('hmsetAsync returns : ' + JSON.stringify(res));

        }).catch(function (err) {
          console.log('error processing multi hmsetAsync : ' + err);
        });
      }
      pclientMulti.exec(function (err) {
        if (err) console.log('error executing multi ' + err)
          console.log('Dieded');
          pclient.quit();
        })
    }).catch(function (err) {
      console.log('Error processing querie(s) : ' + err.stack);
    })
  } catch (err) {
    console.log('error running test : ' + err)
    throw err;
  }
});






