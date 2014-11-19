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
 * TAsyncCommonSocketChannel.cpp
 *
 *  Created on: Apr 16, 2014
 *      Author: oarowojolu
 */

#include <ezbake/thriftutils/tasync/TAsyncCommonSocketChannel.h>

#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TProtocolException.h>

namespace ezbake { namespace thriftutils { namespace tasync {

using namespace ::boost::asio;

using ::boost::asio::ip::tcp;
using ::boost::system::error_code;

using ::apache::thrift::transport::TTransportException;
using ::apache::thrift::protocol::TProtocolException;
using ::apache::thrift::transport::TMemoryBuffer;



log4cxx::LoggerPtr const TAsyncCommonSocketChannel::LOG = log4cxx::Logger::getLogger("::ezbake::thriftutils::tasync::TAsyncCommonSocketChannel");


void TAsyncCommonSocketChannel::connect(const ReturnCallback& cb) {
    std::ostringstream ss;
    ss << _port;
    tcp::resolver::query query(_host, ss.str());
    _resolver.async_resolve(query,
            _strand.wrap(::boost::bind(&TAsyncCommonSocketChannel::handleResolve,
                    this,
                    placeholders::error,
                    placeholders::iterator,
                    cb)));
}


void TAsyncCommonSocketChannel::sendMessage(const VoidCallback& cb,
        ::apache::thrift::transport::TMemoryBuffer* message) {
    (void)cb; (void)message;
    BOOST_THROW_EXCEPTION(::apache::thrift::TException(
            "Unexpected call to TAsyncCommonSocketChannel::sendMessage(VoidCallback ....)"));
}


void TAsyncCommonSocketChannel::recvMessage(const VoidCallback& cb,
        ::apache::thrift::transport::TMemoryBuffer* message) {
    (void)cb; (void)message;
    BOOST_THROW_EXCEPTION(::apache::thrift::TException(
            "Unexpected call to TAsyncCommonSocketChannel::recvMessage(VoidCallback ....)"));
}


void TAsyncCommonSocketChannel::sendAndRecvMessage(const VoidCallback& cb,
        ::apache::thrift::transport::TMemoryBuffer* sendBuf_ptr,
        ::apache::thrift::transport::TMemoryBuffer* recvBuf_ptr) {

    if (NULL == sendBuf_ptr) {
        BOOST_THROW_EXCEPTION(::apache::thrift::TException("sendBuf_ptr is NULL"));
    }

    if (NULL == recvBuf_ptr) {
        BOOST_THROW_EXCEPTION(::apache::thrift::TException("recvBuf_ptr is NULL"));
    }

    ReturnCallback surrogateCb = ::boost::bind(&TAsyncCommonSocketChannel::handleSendAndRecvMessageCompletion,
                                               this, cb, _1);

    ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer> sendBuf(sendBuf_ptr, SharedPtrNullDeleter());
    ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer> recvBuf(recvBuf_ptr, SharedPtrNullDeleter());

    sendAndRecvMessageWithReturnCallback(surrogateCb, sendBuf, recvBuf);
}


void TAsyncCommonSocketChannel::handleSendAndRecvMessageCompletion(const VoidCallback& principalCb,
        const ::boost::shared_ptr< ::apache::thrift::transport::TTransportException>& err) {
    if (err) {
        LOG4CXX_ERROR(LOG, "SendAndRecvMessage dispatch error: " << err->what());
    }

    principalCb();
}


void TAsyncCommonSocketChannel::handleConnect(const error_code& err,
        tcp::resolver::iterator epItr, const ReturnCallback& cb) {
    ::boost::shared_ptr<TTransportException> exRtn;

    if (!err) {
        //connect was successful. Inform handler using exception shared_ptr that's not initialized
        cb(exRtn);
    } else if (epItr != tcp::resolver::iterator()) {
        //Connection failed. Try the next endpoint we resolved
        connectToResolvedEndpoint(epItr, cb);
    } else {
        //Connection failed and no more endpoints to try, report error
        exRtn = ::boost::make_shared<TTransportException>("TAsyncCommonSocketChannel failed after trying all endpoints");
        cb(exRtn);
    }
}


void TAsyncCommonSocketChannel::handleResolve(const error_code& err,
        tcp::resolver::iterator epItr, const ReturnCallback& cb) {
    if (err) {
        cb(::boost::make_shared<TTransportException>("TAsyncCommonSocketChannel error in handling resolve: " + err.message()));
    } else {
        //attempt a connection on the first endpoint resolved
        connectToResolvedEndpoint(epItr, cb);
    }
}


}}} // namespace ::ezbake::thriftutils::tasync
