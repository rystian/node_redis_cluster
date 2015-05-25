var Client = require('./Client');

module.exports = {
  clusterClient : {
    clusterInstance: function(discovery_address, cb) {
      var client = new Client(discovery_address);
      client.connect(function(err) {
        if (err) return cb(err);
        cb(null, client);
      });
    },
    createClient : function(discovery_address) {
        var client = new Client(discovery_address);
        client.connect(function(err) {
            if (err) throw new Error(err);
            return client;
        });
    }
  }
};
