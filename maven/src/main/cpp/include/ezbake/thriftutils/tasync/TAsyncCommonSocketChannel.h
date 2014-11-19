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
 * TAsyncCommonSocketChannel.h
 *
 *  Created on: Apr 16, 2014
 *      Author: oarowojolu
 */

#ifndef EZBAKE_THRIFTUTILS_TASYNC_TASYNCCOMMONSOCKETCHANNEL_H_
#define EZBAKE_THRIFTUTILS_TASYNC_TASYNCCOMMONSOCKETCHANNEL_H_

#include <string>
#include <log4cxx/logger.h>
#include <boost/shared_ptr.hpp>
#include <thrift/async/TAsyncChannel.h>
#include <ezbake/thriftutils/tasync/TAsyncAsioTransport.h>



namespace ezbake { namespace thriftutils { namespace tasync {

class TAsyncCommonSocketChannel: public ::apache::thrift::async::TAsyncChannel {
public:
    using TAsyncChannel::VoidCallback;
    typedef ::boost::function<void(const ::boost::shared_ptr< ::apache::thrift::transport::TTransportException>&)> ReturnCallback;

public:
    /**
     * Constructor
     */
    TAsyncCommonSocketChannel(const ::std::string& host, unsigned int port,
            ::boost::asio::io_service& ioService) :
        _host(host), _port(port),
        _strand(ioService), _resolver(ioService),
        _otrans(::boost::make_shared<TAsioOutputTransport>(_strand)),
        _itrans(::boost::make_shared<TAsioInputTransport>(_strand))
    {}

    /**
     * Destructor
     */
    virtual ~TAsyncCommonSocketChannel() {}

    /**
     * Connect to the remote host
     */
    virtual void connect(const ReturnCallback& cb);

    /**
     * Close the channel
     *
     * @throws TTransportException on error in closing underlying socket
     */
    virtual void close() = 0;

    /**
     * Channel States
     */
    virtual bool good() const = 0;
    virtual bool error() const {
        return !this->good();
    }
    virtual bool timedOut() const {
        return false;
    }

    /**
     * Send a message over the channel.
     */
    virtual void sendMessage(const VoidCallback& cb, ::apache::thrift::transport::TMemoryBuffer* message);

    /**
     * Receive a message from the channel.
     */
    virtual void recvMessage(const VoidCallback& cb, ::apache::thrift::transport::TMemoryBuffer* message);

    /**
     * Send a message over the channel and receive a response.
     */
    virtual void sendAndRecvMessage(const VoidCallback& cb,
            ::apache::thrift::transport::TMemoryBuffer* sendBuf,
            ::apache::thrift::transport::TMemoryBuffer* recvBuf);

    /**
     * Send a message over the channel.
     */
    virtual void sendMessageWithReturnCallback(const ReturnCallback& cb,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& message) = 0;

    /**
     * Receive a message from the channel.
     */
    virtual void recvMessageWithReturnCallback(const ReturnCallback& cb,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& message) = 0;

    /**
     * Send a message over the channel and receive a response.
     */
    virtual void sendAndRecvMessageWithReturnCallback(const ReturnCallback& cb,
            const ::boost::shared_ptr< apache::thrift::transport::TMemoryBuffer>& sendBuf,
            const ::boost::shared_ptr< apache::thrift::transport::TMemoryBuffer>& recvBuf) = 0;

    /**
     * Callback to handle response from asynchronous call to connect
     *
     * @param err   error code return from asio async call
     * @param epItr endpoint interator connect was attempted on
     * @param cb   callback object for dispatching result
     */
    virtual void handleConnect(const ::boost::system::error_code& err,
            ::boost::asio::ip::tcp::resolver::iterator epItr,
            const ReturnCallback& cb);

    /**
     * Handler to handle asynchronous completion of SendAndRecvMessage
     */
    void handleSendAndRecvMessageCompletion(const VoidCallback& delegateCob,
            const ::boost::shared_ptr< ::apache::thrift::transport::TTransportException>& err);


protected:
    /**
     * Initiates the a connection to the specified endpoint on the underlying socket
     *
     * @param epItr     endpoint iterator to attempt connection on
     * @param cb       callback object for dispatching result.
     */
    virtual void connectToResolvedEndpoint(::boost::asio::ip::tcp::resolver::iterator epItr,
            const ReturnCallback& cb) = 0;


    template <typename AsioStream>
    void sendMessageOnStream(const ::boost::shared_ptr<AsioStream>& stream, const ReturnCallback& cb,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& message) {
        _otrans->writeFromBuffer(stream, message, cb);
    }
    template <typename AsioStream>
    void recvMessageOnStream(const ::boost::shared_ptr<AsioStream>& stream, const ReturnCallback& cb,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& message) {
        _itrans->readToBuffer(stream, message, cb);
    }
    template <typename AsioStream>
    void sendAndRecvMessageOnStream(const ::boost::shared_ptr<AsioStream> stream, const ReturnCallback& cb,
            const ::boost::shared_ptr< apache::thrift::transport::TMemoryBuffer>& sendBuf,
            const ::boost::shared_ptr< apache::thrift::transport::TMemoryBuffer>& recvBuf) {
        _otrans->writeFromBuffer(stream, sendBuf,
                _strand.wrap(::boost::bind(&TAsyncCommonSocketChannel::handleSendAndRecv<AsioStream>,
                                           this, _1, stream, cb, recvBuf)));
    }

    /**
     * Callback to handle response from asynchronous call to resolve host endpoints
     *
     * @param err   error code return from asio async call
     * @param epItr endpoint interator resolve was attempted on
     * @param cb   callback object for dispatching result
     */
    virtual void handleResolve(const ::boost::system::error_code& err,
            ::boost::asio::ip::tcp::resolver::iterator epItr,
            const ReturnCallback& cb);

    /**
     * Callback to handle the response from asynchronous SendMessage from a
     * SendAndRecvMessage call
     *
     * @param exceptionRtn   return exception shared pointer
     * @param stream         asio stream to read from
     * @param cb            callback for dispatching the results
     * @param recvBuf        memory buffer for receiving the message
     */
    template <typename AsioStream>
    void handleSendAndRecv(const ::boost::shared_ptr< ::apache::thrift::transport::TTransportException>& exceptionRtn,
            const ::boost::shared_ptr<AsioStream>& stream, const TAsioTransport::Callback& cb,
            const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& recvBuf) {
        if (exceptionRtn) {
            //failure in sending message. Report an error to the originator
            ::std::string errorMessage = "Failure in sending message: ";
            cb(::boost::make_shared< ::apache::thrift::transport::TTransportException>(errorMessage + exceptionRtn->what()));
        } else {
            //Successfully sent message, dispatch call to receive the response
            recvMessageOnStream(stream, cb, recvBuf);
        }
    }

protected:
    static log4cxx::LoggerPtr const LOG;

    ::std::string _host;
    unsigned int _port;

    /*
     * Event handlers need to be dispatched with strands (::boost::asio::io_service::strand) to
     * ensure strict sequential invocation of the event handlers within an Async Channel.
     */
    ::boost::asio::io_service::strand _strand;

    ::boost::asio::ip::tcp::resolver _resolver;

    ::boost::shared_ptr<TAsioOutputTransport> _otrans;
    ::boost::shared_ptr<TAsioInputTransport> _itrans;
};

/**
 * Custom shared_ptr deleter that allows us to create a shared_ptr to an already existing object,
 * so that the shared_ptr does not attempt to destroy the object when there are no more references left.
 */
struct SharedPtrNullDeleter {
    void operator()(void const *) const {}
};

}}} // namespace ::ezbake::thriftutils::tasync

#endif /* EZBAKE_THRIFTUTILS_TASYNC_TASYNCCOMMONSOCKETCHANNEL_H_ */
