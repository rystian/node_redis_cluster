var EventEmitter = require('events').EventEmitter;
var util = require('util');

var redis = require('redis');

var hashSlot = require('./hashSlot');
var commands = require('./lib/commands');

function Client(discovery_address) {
  EventEmitter.call(this);

  this.discovery_address = discovery_address;
}
util.inherits(Client, EventEmitter);

Client.prototype.connect = function (cb) {
  var self = this;
  self.onConnect = self.onConnect || [];
  self.onConnect.push(cb);

  if (self.connecting) return;

  self.connecting = true;
  self.discoverTopology(function (err) {
    self.connecting = false;
    if (err) return callback(err);
    self.bind();
    callback();
  });

  function callback(err) {
    var callbacks = self.onConnect;
    self.onConnect = null;
    callbacks.forEach(function(cb) {
      cb(err);
    });
  }
};

Client.prototype.reconnect = function (cb) {
  var self = this;
  if (self.nodes) {
    self.nodes.forEach(function(node) {
      node.link.end();
    })
  }
  self.connect(cb);
};

Client.prototype.getSlot = function (key) {
  if (!key) return;
  return hashSlot(key);
};

Client.prototype.getNode = function (key) {
  var self = this;
  if (!self.nodes) return;
  var slot = self.getSlot(key);
  if (!slot) return;
  var l = self.nodes.length;
  for (var i = 0; i < l; i++) {
    var node = self.nodes[i];
    if (node && node.slots && node.slots[0] <= slot && slot <= node.slots[1])
      return node;
  }
};

Client.prototype.discoverTopology = function (cb) {
  var self = this;
  self.nodes = [];
  var fire_starter = connectToLink(self.discovery_address);
  fire_starter.cluster('nodes', function(err, nodes) {
    // workaround which allows redis-cluster to work when not in cluster mode
    if(err && err.indexOf('cluster support disabled') !== -1) {
      err = null;
      var addr = self.discovery_address;
      nodes = '0000000000000000000000000000000000000000 ' + addr + ' myself,master - 0 0 1 connected 0-16383\n';
    }

    if (err) return cb(err);

    var lines = nodes.split('\n');
    if (lines[lines.length - 1] === '') lines.pop();
    var n = lines.length;

    while (n--) {
      var items = lines[n].split(' ');
      var name = items[0];
      var link = items[1];
      var flags = items[2];
      var state = items[7];

      // don't connect to slaves
      if (flags === 'slave' || flags === 'myself,slave') {
        continue;
      }

      // don't connect to nodes that are not connected to the cluster
      if (state !== 'connected') {
        continue;
      }

      // parse slots
      var slots = [];
      if (lines.length === 1) {
        slots.push(0, 16383);
      } else {
        for(var i = 8; i<items.length;i++) {
          if(items[i].indexOf('-<-') !== -1 || items[i].indexOf('->-') !== -1) {
            //migrate in process...
            continue;
          }
          if(items[i].indexOf('-') === -1) {
            slots.push(Number(items[i]), Number(items[i]));
            continue;
          }
          var t = items[i].split('-');
          slots.push(Number(t[0]), Number(t[1]));
        }
      }

      self.nodes.push({
        name: name,
        connectStr: link,
        link: connectToLink(link),
        slots: slots
      });
    }

    cb();
  });
};

Client.prototype.bind = function() {
  var self = this;

  self.nodes.forEach(function(node) {
    node.link.on('error', onError.bind(node));
  });

  var c = commands.length;
  while (c--) {
    (function (command) {
      self[command] = function () {
        var o_arguments = Array.prototype.slice.call(arguments);
        var orig_arguments = Array.prototype.slice.call(arguments);
        var o_callback;
        var last_used_node;
        var redirections = 0;

        // Taken from code in node-redis.
        var last_arg_type = typeof o_arguments[o_arguments.length - 1];
        if (last_arg_type === 'function') {
          o_callback = o_arguments.pop();
        }

        //for commands such as PING use slot 0
        var slot = o_arguments[0] ? hashSlot(o_arguments[0]) : 0;

        var i = self.nodes.length;
        while (i--) {
          var node = self.nodes[i];
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
        self.reconnect(function(err) {
          if (err) {
            self.emit('error', err);
          }
        });

        function callNode(node) {
          last_used_node = node;
          node.link[command].apply(node.link, o_arguments.concat([callback]));
        }

        function callback(err, data){
          if(err) {
            // Need to handle here errors '-ASK' and '-MOVED'
            // http://redis.io/topics/cluster-spec

            // ASK error example: ASK 12182 127.0.0.1:7001
            // When we got ASK error, we need just repeat a request on right node with ASKING command
            // If after ASK we got MOVED err, thats mean no key found
            if(err.toString().substr(0, 3)==='ASK') {
              if(redirections++ > 5) {
                if(o_callback)
                  o_callback(new Error('Too much redirections'));
                return;
              }
              //console.log('ASK redirection')
              var connectStr = err.split(' ')[2];
              var node = null;
              for(var i=0;i<self.nodes.length;i++) {
                if(self.nodes[i].connectStr === connectStr) {
                  node = self.nodes[i];
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
            } else if(err.toString().substr(0, 5) === 'MOVED') {
              //MOVED error example: MOVED 12182 127.0.0.1:7002
              //this is our trigger when cluster topology is changed
              self.reconnect(function(err) {
                if (err) {
                  if (o_callback)
                    o_callback(err);
                  return;
                }
                //repeat command
                self[command].apply(self, orig_arguments);
              });
              return;
            }
          }
          if(o_callback)
            o_callback(err, data);
        }
      };
    })(commands[c]);
  }

  function onError(err) {
    self.emit('error', err);
    var base_wait = 1000;

    if (err && err.toString().indexOf("ECONNREFUSED") >= 0 && !client.reconnecting) {
      var retries = 0;
      var wait = base_wait;
      recover();
    }

    function recover() {
      console.error('Got ECONNREFUSED, reconnecting in ' + wait + ' ms');
      setTimeout(function() {
        console.log('debug: reconnecting');
        self.reconnect(function (err) {
          if (err) {
            wait = Math.min(30000, base_wait * Math.pow(2, ++retries));
            recover();
          }
        });
      }, wait);
    }
  }
};

function connectToLink(str, auth, options) {
  var spl = str.split(':');
  options = options || {};

  var c = redis.createClient(spl[1], spl[0], options);
  if (auth)
    c = c.auth(auth);

  return c;
}

module.exports = Client;
