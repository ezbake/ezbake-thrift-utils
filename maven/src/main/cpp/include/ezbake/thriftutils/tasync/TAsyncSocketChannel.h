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
 * TAsyncSocketChannel.h
 *
 *  Created on: Apr 16, 2014
 *      Author: oarowojolu
 */

#ifndef EZBAKE_THRIFTUTILS_TASYNC_TASYNCSOCKETCHANNEL_H_
#define EZBAKE_THRIFTUTILS_TASYNC_TASYNCSOCKETCHANNEL_H_

#include <ezbake/thriftutils/tasync/TAsyncCommonSocketChannel.h>
#include <ezbake/thriftutils/tasync/TAsyncClientManager.h>



namespace ezbake { namespace thriftutils { namespace tasync {

class TAsyncSocketChannel : public TAsyncCommonSocketChannel {
public:
    /**
     * Destructor
     */
    virtual ~TAsyncSocketChannel();

    /**
     * Synchronously shut down the underlying socket
     *
     * @throws std::exception on failure
     */
    virtual void close();

    /**
     * Check state of channel
     * Returns true if the underlying socket is open
     */
    virtual bool good() const {
        return _socket->is_open();
    }

    virtual void sendMessageWithReturnCallback(const ReturnCallback& cb,
      const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& message) {
        this->sendMessageOnStream(_socket, cb, message);
    }

    virtual void recvMessageWithReturnCallback(const ReturnCallback& cb,
      const ::boost::shared_ptr< ::apache::thrift::transport::TMemoryBuffer>& message) {
        this->recvMessageOnStream(_socket, cb, message);
    }

    virtual void sendAndRecvMessageWithReturnCallback(const ReturnCallback& cb,
      const ::boost::shared_ptr< apache::thrift::transport::TMemoryBuffer>& sendBuf,
      const ::boost::shared_ptr< apache::thrift::transport::TMemoryBuffer>& recvBuf) {
        this->sendAndRecvMessageOnStream(_socket, cb, sendBuf, recvBuf);
    }

protected:
    /**
     * Constructor
     */
    TAsyncSocketChannel(const std::string& host,
            unsigned int port, ::boost::asio::io_service& ioService);

private:
    /**
     * Callback handler to connect to resolved endpoint
     */
    void connectToResolvedEndpoint(::boost::asio::ip::tcp::resolver::iterator epItr,
            const ReturnCallback& cb);

private:
    ::boost::shared_ptr< ::boost::asio::ip::tcp::socket> _socket;

    friend class TAsyncSocketChannelFactory;
};


////

class TAsyncSocketChannelFactory {
public:
    /**
     * Construct a Socket Channel Factory
     */
    TAsyncSocketChannelFactory() :
        _manager(::boost::make_shared<TAsyncClientManager>())
    {}

    /**
     * Construct a Socket Channel Factory with the specified client manager
     */
    TAsyncSocketChannelFactory(const ::boost::shared_ptr<TAsyncClientManager>& manager) :
        _manager(manager)
    {}

    /**
     * Destructor
     */
    virtual ~TAsyncSocketChannelFactory() {}

    /**
     * @returns the manager used in this socket channel factory
     */
    virtual ::boost::shared_ptr<TAsyncClientManager> getManager() const {
        return _manager;
    }

    /**
     * Create an instance of a TAsyncSocketChannel to specified endpoint
     *
     * @param host  remote host to connect to
     * @oaram port  remote port to connect to
     *
     * @return initialized SSL socket channel
     */
    ::boost::shared_ptr<TAsyncSocketChannel> createSocketChannel(const std::string& host,
                unsigned int port) {
        //note: we cannot make use of boost::make_shared since the constructor of TAsyncSocketChannel is protected
        ::boost::shared_ptr<TAsyncSocketChannel> channel(new TAsyncSocketChannel(host, port, _manager->getIoService()));
        return channel;
    }

private:
    ::boost::shared_ptr<TAsyncClientManager> _manager;
};

}}} // namespace ::ezbake::thriftutils::tasync

#endif /* EZBAKE_THRIFTUTILS_TASYNC_TASYNCSOCKETCHANNEL_H_ */
