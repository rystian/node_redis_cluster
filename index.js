var redis = require('redis');

var Client = require('./Client');
var redisClusterSlot = require('./redisClusterSlot');
var commands = require('./lib/commands');

var connectToLink = function(str, auth, options) {
  var spl = str.split(':');
  options = options || {};

  var c = redis.createClient(spl[1], spl[0], options).on('error', function(err) {
    console.error('oops, there was a redis error:', err);
  });

  if (auth)
    c = c.auth(auth);

  return c;
};

/*
  Connect to a node of a Redis Cluster, discover the other nodes and
  respective slots with the "CLUSTER NODES" command, connect to them
  and return an array of the links to all the nodes in the cluster.
*/
function connectToNodesOfCluster (firstLink, callback) {
  var redisLinks = [];
  var fireStarter = connectToLink(firstLink);
  var clusterFn = fireStarter.cluster.bind(fireStarter);
  clusterFn('nodes', function(err, nodes) {
    if(err && err.indexOf('cluster support disabled') !== -1) {
      err = null;
      var port = firstLink.split(':').pop();
      nodes = '0000000000000000000000000000000000000000 :'+port+' myself,master - 0 0 1 connected 0-16383\n';
    } else if (err) {
      callback(err, null);
      return;
    }
    var lines = nodes.split('\n');
    var n = lines.length -1;
    while (n--) {
      var items = lines[n].split(' ');
      var name = items[0];
      var link = items[1];
      var flags = items[2];
      if(flags === 'slave' || flags === 'myself,slave') {
          if (n === 0) {
            callback(err, redisLinks);
            return;
          } 
          continue;
      }
      //var lastPingSent = items[4];
      //var lastPongReceived = items[5];
      var linkState = items[7];

      if (lines.length === 1 && lines[1] === '') {
        var slots = [0, 16383]
      } else {
        var slots = [];
        for(var i = 8; i<items.length;i++) {
          if(items[i].indexOf('-<-') !== -1 || items[i].indexOf('->-') !== -1) {
            //migrate in process...
            continue;
          }
          if(items[i].indexOf('-') === -1) {
            slots.push(items[i], items[i]);
            continue;
          }
          var t = items[i].split('-');
          slots.push(t[0], t[1]);
        }
      }
      
      if (linkState === 'connected') {
        redisLinks.push({
          name: name,
          connectStr: link,
          link: connectToLink(link), 
          slots: slots
        });
      }
      if (n === 0) {
        callback(err, redisLinks);
      }
    }
  });
}

function bindCommands (nodes, oldClient) {
  var client = oldClient || new Client();
  client.nodes = nodes;
  //catch on error from nodes
  function onError(err) {
    client.emit('error', err);
    var base_wait = 1000;

    if (err && err.toString().indexOf("ECONNREFUSED") >= 0 && !client.reconnecting) {
      client.reconnecting = true;
      var retries = 0;
      var wait = base_wait;
      recover();
    }

    function recover() {
      console.error('Got ECONNREFUSED, reconnecting in ' + wait + ' ms');
      setTimeout(function() {
        reconnectClient(client.fireStarter, client, function(err, newClient) {
          if (err) {
            wait = Math.min(30000, base_wait * Math.pow(2, ++retries));
            recover();
            return;
          }
          client = newClient;
          client.reconnecting = false;
        });
      }, wait);
    }
  }
  for(var i=0;i<nodes.length;i++) {
    nodes[i].link.on('error', onError.bind(nodes[i]));
  }
  var c = commands.length;
  while (c--) {
    (function (command) {
      client[command] = function () {
        var o_arguments = Array.prototype.slice.call(arguments);
        var orig_arguments = Array.prototype.slice.call(arguments);
        var o_callback;
        var lastusednode;
        
        // Taken from code in node-redis.
        var last_arg_type = typeof o_arguments[o_arguments.length - 1];

        if (last_arg_type === 'function') {
          o_callback = o_arguments.pop();
        }

        //for commands such as PING use slot 0
        var slot = o_arguments[0] ? redisClusterSlot(o_arguments[0]) : 0;
        
        var redirections = 0;
        
        function callback(e, data){
          if(e) {
            // Need to handle here errors '-ASK' and '-MOVED'
            // http://redis.io/topics/cluster-spec
            
            // ASK error example: ASK 12182 127.0.0.1:7001
            // When we got ASK error, we need just repeat a request on right node with ASKING command
            // If after ASK we got MOVED err, thats mean no key found
            if(e.toString().substr(0, 3)==='ASK') {
              if(redirections++ > 5) {
                if(o_callback)
                  o_callback(new Error('Too much redirections'));
                return;
              }
              //console.log('ASK redirection')
              var connectStr = e.split(' ')[2];
              var node = null;
              for(var i=0;i<nodes.length;i++) {
                if(nodes[i].connectStr === connectStr) {
                  node = nodes[i];
                  break;
                }
              }
              if(node) {
                node.link.send_command('ASKING', [], function(){});
                return callNode(node, true);
              }
              if(o_callback)
                o_callback(new Error('Requested node for redirection not found `' + connectStr + '`'));
              return;
            } else if(e.toString().substr(0, 5) === 'MOVED') {
              //MOVED error example: MOVED 12182 127.0.0.1:7002
              //this is our trigger when cluster topology is changed
              reconnectClient(lastusednode.connectStr, client, function(err, newClient) {
                if (err) {
                  if (o_callback)
                    o_callback(err);
                  return;
                }
                client = newClient;
                //repeat command
                client[command].apply(client, orig_arguments);
              });
              return;
            }
          }
          if(o_callback)
            o_callback(e, data);
        }
        
        var i = nodes.length;
        while (i--) {
          var node = nodes[i];
          var slots = node.slots;
          for(var r=0;r<slots.length;r+=2) {
            if ((slot >= slots[r]) && (slot <= slots[r+1])) {
              callNode(node);
              return;
            }
          }
        }

        if (o_callback)
          o_callback(new Error('slot '+slot+' found on no nodes'));

        // unable to find node for slot so we reconnect
        reconnectClient(client.fireStarter, client, function(err, newClient) {
          if (err) {
            client.emit('error', err);
            return;
          }
          client = newClient;
        });
        
        function callNode(node) {
          lastusednode = node;
          node.link[command].apply(node.link, o_arguments.concat([callback]));
        }
      };
    })(commands[c]);
  }
  return(client);
}

function reconnectClient(firstLink, client, callback) {
  if (typeof client === 'function') {
    callback = client;
    client = null;
  }

  if (client && client.nodes) {
    client.nodes.forEach(function(node) {
      node.link.end();
    })
  }

  connectToNodesOfCluster(firstLink, function (err, nodes) {
    if(err) return callback(err);
    client = bindCommands(nodes, client);
    client.fireStarter = firstLink;
    callback(null, client);
  });
}

module.exports = {
    clusterClient : {
      clusterInstance: reconnectClient
    }
};
