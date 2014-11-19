/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

var fs = require('fs');
var path = require('path');
var tls = require('tls');
var net = require('net');
var poolModule = require('generic-pool');
var _ = require('lodash-node');
var logger = require('winston');
var EzConfiguration = require("ezbake-configuration");

var cons = require('./constants');

var thrift = require('thrift');
var ttransport = require('thrift/lib/thrift/transport');
var TBinaryProtocol = require('thrift/lib/thrift/protocol').TBinaryProtocol;

var connectionPools = {};
var securityIds = {};

function ThriftUtils(ezConfiguration){
  if(!ezConfiguration){
    this.ezConfig = new EzConfiguration.EzConfiguration();
  } else {
    this.ezConfig = ezConfiguration;
  }
  var zookeeperHelper = new EzConfiguration.helpers.ZookeeperConfiguration(this.ezConfig);
  var EzDiscovery = require("ezbake-discovery").DiscoveryClient;
  this.discoveryClient = new EzDiscovery(zookeeperHelper.getConnectionString());

  this.useSsl = this.ezConfig.getBoolean('thrift.use.ssl', false);
  if (this.useSsl) {
      // Load certs up front if using ssl
      this._getAppCert();
      this._getPrivatePEM();
      this._getCACert();
  }
}

ThriftUtils.prototype.getConnection = function(appName, serviceName, options, cb){
  if(cb === undefined){
    if(typeof options === 'function'){
      cb = options;
      options = {};
    } else {
      throw 'A callback must be given to getConnection';
    }
  }
  if(connectionPools[appName+serviceName] === undefined){
    connectionPools[appName+serviceName] = this._createPool(appName, serviceName, options);
  }
  connectionPools[appName+serviceName].acquire(function(err, client){
    if(err){
      cb(err);
      return;
    }
    var close = function(){
      connectionPools[appName+serviceName].release(client);
    };
    cb(undefined, client, close);
  });
};

//If created connection with min connections > 0 this must be called before calling applications closes.
ThriftUtils.prototype.closeAllConnectionsForApp = function(appName, serviceName, optionalCallback){
  if(connectionPools[appName+serviceName] !== undefined){
    connectionPools[appName+serviceName].drain(function(){
      connectionPools[appName+serviceName].destroyAllNow(optionalCallback);
    });
  }
};

ThriftUtils.prototype.getSecurityId = function(applicationOrServiceName, cb){
  if(securityIds[applicationOrServiceName]) return cb(undefined, securityIds[applicationOrServiceName]);
  var _self = this;
  this.discoveryClient.isCommonService(applicationOrServiceName, function(err, isCommonService){
    if(err){
      return cb(err);
    }
    if(isCommonService){
      _self.discoveryClient.getSecurityIdForCommonService(applicationOrServiceName, function(err, securityId) {
        if(err) {
          cb(err);
        } else {
          securityIds[applicationOrServiceName] = securityId;
          cb(undefined, securityId);
        }
      });
    } else {
      _self.discoveryClient.getSecurityIdForApplication(applicationOrServiceName, function(err, securityId){
        if(err){
          cb(err);
        } else {
          securityIds[applicationOrServiceName] = securityId;
          cb(undefined, securityId);
        }
      });
    }
  });
};

ThriftUtils.prototype._createPool = function(appName, serviceName, options){
  var _self = this;
  options = options||{};
  var thriftOptions = _.extend(options, options.thriftOptions);

  var createFunction = function(cb){
    _self.discoveryClient.getEndpoints(appName, serviceName, function(err, children) {
      if(err) {
        logger.error(err);
        cb(new Error('Unable to establish a connection.'));
        return;
      }
      if(children.length === 0) {
        logger.error('Unable to find host and port to make a connection');
        cb(new Error('Unable to establish a connection.'));
        return;
      }

      var endpoints = [];
      _.each(children, function(child){
          if(typeof child !== 'string') return;
          var host_port = child.split(':');
          if(host_port.length === 2){
              endpoints.push({host: host_port[0], port: parseInt(host_port[1], 10)});
          }
      });

      thriftOptions.useSsl = thriftOptions.useSsl || _self.useSsl;
      _self._createConnection(endpoints, thriftOptions, function(err, connection) {
          if (connection) {
            connection.on('error', function(err) {
              logger.error(err);
            });
            connection.on('end', function(){
              logger.debug('connection closed for AppName: ' + appName + ', serviceName: ' + serviceName);
            });
          }
        cb(err, connection);
      });

    });
  };

  var defaults = {
    name     : appName + '-' +serviceName,
    create   : createFunction,
    destroy  : function(client) { client.end(); },
    max      : 10,
    // optional. if you set this, make sure to drain()
    min      : 0,
    // specifies how long a resource can stay idle in pool before being removed
    idleTimeoutMillis : 30000,
    // if true, logs via console.log - can also be a function
    log : function(message, level){logger.log(level, message);}
  };

  extend(options, defaults);
  delete defaults.thriftOptions;
  return poolModule.Pool(defaults);
};

ThriftUtils.prototype._createSSLConnection = function _createSSLConnection(endpoints, options, cb){
    if (typeof options == 'object') {
        options.useSsl = true;
    }
    this._createConnection(endpoints, options, cb);
};

ThriftUtils.prototype._createConnection = function _createConnection(endpoints, options, cb){
    if (typeof options == 'function') {
        cb = options;
        options = {};
    }
    if(!options.transport){
        options.transport = ttransport.TBufferedTransport;
    }
    if(!options.protocol){
        options.protocol = TBinaryProtocol;
    }
    if (options.useSsl) {
        if(!options.cert){
            options.cert = this._getAppCert();
        }
        if(!options.key){
            options.key = this._getPrivatePEM();
        }
        if(!options.ca){
            options.ca = this._getCACert();
        }
        if(!options.rejectUnauthorized){
            options.rejectUnauthorized = false;
        }
    }
    tryConnectEndpoints(endpoints, options, cb);
};

function tryConnectEndpoints(endpoints, options, cb) {
    if (!endpoints.length || endpoints.length < 1) {
        cb("ThriftUtils: No more endpoints to try");
    }
    var ep_copy = endpoints.slice(0);
    var endpoint = ep_copy.splice(randomInt(0, endpoints.length), 1)[0];
    var host = endpoint.host;
    var port = endpoint.port;
    logger.debug("ThriftUtils: Trying connection to %j", endpoint);

    var stream = (options.useSsl)? tls.connect(port, host, options) : net.createConnection(port, host);
    var connection = new thrift.Connection(stream, options);
    connection.host = endpoint.host;
    connection.port = endpoint.port;

    // We set an initial timeout. If the connection times out before the
    // connection has been connected, try the next endpoint
    stream.setTimeout(options.connect_timeout || 0, function(err) {
        if (!connection.connected) {
            connection.emit('error', 'timeout. '+options.connect_timeout+' elapsed before connect');
        }
    });

    var ended;
    var nextEndpointCb = function nextEndpointCb(err) {
        if (!ended && !connection.connected) {
            // set the flag so we don't try the same endpoints multiple times
            ended = true;
            logger.info("ThriftUtils: %s connecting to %s:%d. Trying next. %d endpoints left",
                err, connection.host, connection.port, ep_copy.length);
            tryConnectEndpoints(ep_copy, options, cb);
        } else {
            logger.debug("ThriftUtils: %s connecting to %s:%d.",
                err, connection.host, connection.port);
        }
    };

    var connectCb, connectEvent;
    if (options.useSsl) {
        connectEvent = 'secureConnect';
        connectCb = function secureConnectCb() {
            if (!stream.authorized && stream.authorizationError !=
                "Hostname/IP doesn't match certificate's altnames") {
                logger.error("ThriftUtils: SSL authorization failed. Ending connection");
                connection.emit("error", stream.authorizationError);
            } else {
                logger.debug('ThriftUtils: secure connection accept');
                connection.connected = true;
                this.setTimeout(connection.options.timeout || 0);
                this.setNoDelay();
                this.frameLeft = 0;
                this.framePos = 0;
                this.frame = null;
                connection.offline_queue.forEach(function(data) {
                    connection.connection.write(data);
                });
                cb(null, connection);
                successfulConnect(connection, nextEndpointCb);
                connection.emit("connect");
            }
        };
    } else {
        connectEvent = 'connect';
        connectCb =  function connectCb() {
            logger.debug('ThriftUtils: connection accept');
            cb(null, connection);
	    successfulConnect(connection, nextEndpointCb);
        };
    }

    connection.on('error', nextEndpointCb);
    stream.on(connectEvent, connectCb);
}

/**
 * We have different callbacks for connect and secureConnect,
 * this should be run for both
 */
function successfulConnect(connection, errorCb) {
    connection.removeListener('error', errorCb);
}


/**
 * Load a file from the Ssl Dir
 */
ThriftUtils.prototype._loadSslDirFile = function _loadSslDirFile(fileName) {
    return fs.readFileSync(path.join(this._getSslDir(), fileName), cons.CHAR_SET);
};

/**
 * Wrapper around loading the the private key file to allow for lazy loading
 */
ThriftUtils.prototype._getPrivatePEM = function _getPrivatePEM(){
  if (!this.privatePem) {
    var privateKeyFile = this.ezConfig.getString(cons.SSL.PRIVATE_KEY_FILE);
    if(_.isEmpty(privateKeyFile)) privateKeyFile = cons.SSL.DEFAULT_PRIVATE_KEY_FILE;
    this.privatePem = this._loadSslDirFile(privateKeyFile);
  }
  return this.privatePem;
};

/**
 * Wrapper around loading the application cert to allow for lazy loading
 */
ThriftUtils.prototype._getAppCert = function _getAppCert() {
  if (!this.appCert) {
    var cf = this.ezConfig.getString(cons.SSL.CERT_FILE);
    if(_.isEmpty(cf)) cf = cons.SSL.DEFAULT_CERT_FILE;
    this.appCert = this._loadSslDirFile(cf);
  }
  return this.appCert;
};

/**
 * Wrapper around loading the ca cert to allow for lazy loading
 */
ThriftUtils.prototype._getCACert = function() {
  if (!this.caCert) {
    this.caCert = this._loadSslDirFile(cons.SSL.DEFAULT_CA_FILE);
  }
  return this.caCert;
};

ThriftUtils.prototype._getSslDir = function(){
  if(!this._sslDir){
    this._sslDir = this.ezConfig.getString(cons.SSL.SSL_DIR_KEY, '');
  }
  return this._sslDir;
}

/**
 * Added by Gary Drocella
 * 11/13/14
 */

ThriftUtils.prototype.serializeToBase64 = function(thriftObj) {
  return this.serialize(thriftObj).toString("base64");
}

/**
 * Added by Gary Drocella
 * 11/13/14
 *
 * The logic for serialization was taken from thrift/lib/transport.js
 * TBufferedTransport#flush. It basically writes the object to the
 * buffered transport and then iterates over the out buffer and copies
 * everything into a response buffer
 */
ThriftUtils.prototype.serialize = function(thriftObj) {
  var TBufferedTransport = require('thrift/lib/thrift/transport.js').TBufferedTransport;
  var TBinaryProtocol = new require('thrift/lib/thrift/protocol.js').TBinaryProtocol;

  var transport = new TBufferedTransport();
  var protocol = new TBinaryProtocol(transport);
  thriftObj.write(protocol);

  var msg = new Buffer(transport.outCount);
  var pos = 0;
  transport.outBuffers.forEach(function(buf) {
        buf.copy(msg, pos, 0);
        pos += buf.length;
  });

  return msg;
}

/**
 Extend provides a mixin capability with an optional target allowing it to apply

 @method extend
 @param {Object} obj the source object to mix into another object.
 @param {Object} target (optional) the target object to mix the source into. If elided the Ozone object becomes the target.
 */
function extend(obj, target) {
  target = (target || {});
  if (Object.prototype.toString.call(obj) === "[object Object]") {
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        if (Object.prototype.toString.call(obj[key]) === "[object Object]") {
          if (target[key] === undefined) {
            target[key] = { };
          }
          target[key] = extend(obj[key], target[key]);
        } else if (Object.prototype.toString.call(obj[key]) === "[object Array]") {
          target[key] = (target[key] || []).concat(obj[key]);
        } else {
          target[key] = obj[key];
        }
      }
    }
  }
  return target;
}

function randomInt(low, high) {
  return Math.floor(Math.random()*(high-low)+low);
}

exports.ThriftUtils = ThriftUtils;
exports.server = require('./server');
