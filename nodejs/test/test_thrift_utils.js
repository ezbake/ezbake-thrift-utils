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
var assert = require('assert');
var thrift = require('thrift');
var EzConfiguration = require('ezbake-configuration').EzConfiguration;

// Test utilities for the thrift services
var TestingServer = require('./testing_server');
var PingPong = require('./gen-nodejs/PingPong');
var Paddle = require('./gen-nodejs/pingpong_types').Paddle;

var PingPongHandler = {
    ping: function(result) {
        result(null, "pong");
    }
};

var ThriftUtils = require('..');

describe("thriftutils", function() {
    describe("#serialize", function() {
        it("should serialize with thrift, and have no errors", function() {
            var thriftUtils = new ThriftUtils.ThriftUtils();
            var paddle = new Paddle({
                "name": "pingpongpaddle",
                "flag": false,
                "byyte": 4,
                "counter": 58938,
                "ratio": 44.383,
                "data": new Buffer("BUFFER")
            });

            var serialized = thriftUtils.serialize(paddle);
            assert(serialized != null);
            assert(serialized.length > 0);
        });
    });

    describe("#serializeToBase64", function() {
        it("should serialize with thrift to base 64 and have no errors", function() {
            var thriftUtils = new ThriftUtils.ThriftUtils();
            var paddle = new Paddle({
                "name": "pingpongpaddle",
                "flag": false,
                "byyte": 4,
                "counter": 58938,
                "ratio": 44.383,
                "data": new Buffer("BUFFER")
            });

            var serialized = thriftUtils.serializeToBase64(paddle);
            assert(serialized != null);
            assert(serialized == "CwABAAAADnBpbmdwb25ncGFkZGxlAgACAAMAAwQKAAQAAAAAAADmOgQABUBGMQYk3S8bCwAGAAAABkJVRkZFUgA=");
        });
    });
});

describe("thriftutils", function() {
    var thrift_utils;
    var server_key = fs.readFileSync('test/ssl/server.key');
    var server_cert = fs.readFileSync('test/ssl/server.crt');
    var server, port = 8574;
    var ssl_server, ssl_port = 8575;

    // Start ping pong servers
    before(function() {
        thrift_utils = new ThriftUtils.ThriftUtils(new EzConfiguration());
        server = new TestingServer(PingPong, PingPongHandler, port);
        ssl_server = new TestingServer(PingPong, PingPongHandler, ssl_port, {
            tls: {
                key: server_key,
                cert: server_cert
            }
        });
        server.start();
        ssl_server.start();
    });

    // Stop ping pong servers
    after(function() {
        console.log('running after');
        server.stop();
        ssl_server.stop();
    });

    describe("#_createConnection", function() {
        it("should connect to a normal thrift server", function(done) {
            var endpoints = [
                {host: "localhost", port: port},
                {host: "foreignhost", port: port},
                {host: "foreignhost", port: ssl_port},
                // Right now there is a bug where plain client -> ssl server hangs forever
                // it is due to the fact that the client connects to the server, and then
                // even if the server closes the connection due to the handshake failure
                // the client isn't notified
                //{host: "localhost", port: ssl_port}
            ];
            thrift_utils._createConnection(endpoints, {
                timeout: 100,
                connect_timeout: 50
            },function(err, connection) {
                checkError(err);
                thrift.createClient(PingPong, connection).ping(function(err, response) {
                    checkError(err);
                    assert(response === 'pong');
                    connection.end();
                    done();
                });
            });
        });

        it("should connect to an ssl thrift server", function(done) {
            var endpoints = [
                {host: "localhost", port: port},
                {host: "foreignhost", port: port},
                {host: "localhost", port: ssl_port},
                {host: "foreignhost", port: ssl_port}
            ];
            thrift_utils._createConnection(endpoints, {
                cert: server_cert,
                key: server_key,
                ca: server_cert,
                timeout: 100,
                connect_timeout: 1000,
                useSsl: true
            },function(err, connection) {
                checkError(err);
                thrift.createClient(PingPong, connection).ping(function(err, response) {
                    checkError(err);
                    assert(response === 'pong');
                    connection.end();
                    done();
                });
            });
        });
    })

    describe('#_createSSLConnection', function() {
        it("should connect to an ssl thrift server", function(done) {
            var endpoints = [
                {host: "localhost", port: port},
                {host: "foreignhost", port: port},
                {host: "localhost", port: ssl_port},
                {host: "foreignhost", port: ssl_port}
            ];
            thrift_utils._createSSLConnection(endpoints, {
                cert: server_cert,
                key: server_key,
                ca: server_cert,
                timeout: 100,
                connect_timeout: 1000,
                useSsl: true
            },function(err, connection) {
                checkError(err);
                thrift.createClient(PingPong, connection).ping(function(err, response) {
                    checkError(err);
                    assert(response === 'pong');
                    connection.end();
                    done();
                });
            });
        });
    });

    // To run these tests:
    // start a zkServer on localhost:2181 (override this in before())
    // register:
    //   /ezDiscovery/thriftutils/testssl/endpoints/localhost:8575
    //   /ezDiscovery/thriftutils/test/endpoints/localhost:8574
    // Then remove .skip
    describe.skip('#getConnection', function() {
        var ezconfig;
        beforeEach(function() {
            ezconfig = new EzConfiguration();
            ezconfig.getProperties()['zookeeper.connection.string'] = "localhost:2181";
        });
        it("should connect to a plain thrift server", function(done) {
            ezconfig.getProperties()['thrift.use.ssl'] = "false";
            var thrift_utils = new ThriftUtils.ThriftUtils(ezconfig);
            thrift_utils.getConnection('thriftutils', 'test', function(err, connection) {
                checkError(err);
                thrift.createClient(PingPong, connection).ping(function(err, response) {
                    checkError(err);
                    assert(response === 'pong');
                    connection.end();
                    done();
                });
            });
        });

        it("should connect to an ssl thrift server", function(done) {
            ezconfig.getProperties()['thrift.use.ssl'] = "true";
            var thrift_utils = new ThriftUtils.ThriftUtils(ezconfig);
            thrift_utils.getConnection('thriftutils', 'testssl', {
                cert: server_cert,
                key: server_key,
                ca: server_cert,
                timeout: 100,
                connect_timeout: 50,
            }, function(err, connection) {
                checkError(err);
                thrift.createClient(PingPong, connection).ping(function(err, response) {
                    checkError(err);
                    assert(response === 'pong');
                    connection.end();
                    done();
                });
            });
        });

        it("should accept options in thriftOptions sub-object", function(done) {
            ezconfig.getProperties()['thrift.use.ssl'] = "true";
            var thrift_utils = new ThriftUtils.ThriftUtils(ezconfig);
            thrift_utils.getConnection('thriftutils', 'testssl', {
                thriftOptions: {
                cert: server_cert,
                key: server_key,
                ca: server_cert,
                },
                timeout: 100,
                connect_timeout: 50,
            }, function(err, connection) {
                checkError(err);
                thrift.createClient(PingPong, connection).ping(function(err, response) {
                    checkError(err);
                    assert(response === 'pong');
                    connection.end();
                    done();
                });
            });
        });


    });
});

function checkError(err) {
    if (err) {
        assert(false, err);
    }
}
