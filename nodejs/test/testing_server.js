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

var thrift = require('thrift');
var thrift_server = require('..').server;

/**
 * TestingServer class
 *
 * options:
 *     ssl:
 *         key: server private key (buffer/string)
 *         cert: server certificate (buffer/string)
 */
function TestingServer(tservice, handler, port, options) {
    this.port = port;
    this.options = options || {};

    if (this.options.tls) {
        this.server = thrift_server.createServer(tservice, handler, this.options);
    } else {
        this.server = thrift.createServer(tservice, handler);
    }
}
TestingServer.prototype.start = function startServer() {
    var self = this;
    console.log("server starting on", this.port);
    this.server.listen(this.port);
    this.server.on('listening', function() {
        console.log("server listening on", self.port);
        self.listening = true;
    });
};

TestingServer.prototype.stop = function stopServer() {
    if (this.listening) {
        console.log("stopping server");
        this.server.close();
    }
};

module.exports = TestingServer;
