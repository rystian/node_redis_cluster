/**
 * Created by teggy on 5/25/15.
 */
var clusterClient = require('../index').clusterClient;
var client = clusterClient.createClient('127.0.0.1:7000');
client.on('ready',function(){
    console.log('Client is ready!');
    client.set('foo','bar');
    client.get('foo',function(err,reply){
        if(err) throw new Error(err);
        console.log('replies :'+JSON.stringify(reply));
    })
})
client.on('error',function(){
    console.log('error while connecting');
})
client.connect(function(err){
    if(err) throw new Error(err);
})



