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

/*
 * TAsyncSocketChannel.cpp
 *
 *  Created on: Apr 16, 2014
 *      Author: oarowojolu
 */

#include <ezbake/thriftutils/tasync/TAsyncSocketChannel.h>

#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TProtocolException.h>

namespace ezbake { namespace thriftutils { namespace tasync {

using namespace ::boost::asio;

using ::boost::asio::ip::tcp;
using ::boost::system::error_code;

using ::apache::thrift::transport::TTransportException;
using ::apache::thrift::protocol::TProtocolException;
using ::apache::thrift::transport::TMemoryBuffer;


TAsyncSocketChannel::TAsyncSocketChannel(const std::string& host,
        unsigned int port, ::boost::asio::io_service& ioService) :
    TAsyncCommonSocketChannel(host, port, ioService),
    _socket(::boost::make_shared< ::boost::asio::ip::tcp::socket>(ioService))
{
}


TAsyncSocketChannel::~TAsyncSocketChannel() {
    try {
        close();
    } catch (...) {
        //do nothing
    }
}


void TAsyncSocketChannel::close() {
    if (good()) {
        error_code ec;
        _socket->close(ec);

        if (ec) {
            BOOST_THROW_EXCEPTION(TTransportException("Error in closing underlying socket: " + ec.message()));
        }
    }
}


void TAsyncSocketChannel::connectToResolvedEndpoint(tcp::resolver::iterator epItr,
        const ReturnCallback& cb) {
    tcp::endpoint endpoint = *epItr;
    if (_socket->is_open()) {
        _socket->close();
    }
    _socket->async_connect(endpoint,
            _strand.wrap(::boost::bind(&TAsyncCommonSocketChannel::handleConnect, this,
                    placeholders::error, ++epItr, cb)));
}


}}} // namespace ::ezbake::thriftutils::tasync
