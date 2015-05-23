/**
 * Created by teggy on 5/20/15.
 */
var client = require('../index').clusterClient;
var Promise = require('bluebird');
var pub = null;
var sub = null;
var channelName = 'my-channel';

client.clusterInstance('127.0.0.1:7000',pub_ready);
client.clusterInstance('127.0.0.1:7000',sub_ready);

function pub_ready(err,conn){
    if(err) throw err;
    pub = conn;
}

function sub_ready(err,conn){
    if(err) throw err;
    sub = conn.getLink(channelName);
    sub.on('subscribe',function(channel,count){
        console.log('count : '+count);
        if(pub) {
            pub.publish(channelName,'message number 1');
            pub.publish(channelName,'message number 2');
            pub.publish(channelName,'message number 3');
            pub.publish(channelName,'kill');
        } else {
            console.log('pub is not ready yet!');
        }
    })
    sub.on('message',function(channel, message){
        console.log('pub channel ' + channel + ': ' + message);
        if(message==='kill'){
            sub.unsubscribe();
            sub.quit();
            pub.quit();
        }
    });

    sub.subscribe(channelName);

}







