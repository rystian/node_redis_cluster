var Client = require('./Client');

module.exports = {
  clusterClient : {
    clusterInstance: function(discovery_address, cb) {
      var client = new Client(discovery_address);
      client.connect(cb);
    }
  }
};
