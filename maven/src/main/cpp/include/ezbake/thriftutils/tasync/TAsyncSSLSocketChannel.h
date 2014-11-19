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
 * TAsyncSSLSocketChannel.h
 *
 *  Created on: Apr 16, 2014
 *      Author: oarowojolu
 */

#ifndef EZBAKE_THRIFTUTILS_TASYNC_TASYNCSSLSOCKETCHANNEL_H_
#define EZBAKE_THRIFTUTILS_TASYNC_TASYNCSSLSOCKETCHANNEL_H_

#include <boost/asio/ssl.hpp>
#include <ezbake/thriftutils/tasync/TAsyncCommonSocketChannel.h>
#include <ezbake/thriftutils/tasync/TAsyncClientManager.h>



namespace ezbake { namespace thriftutils { namespace tasync {

class TAsyncSSLSocketChannel: public TAsyncCommonSocketChannel {
public:
    /**
     * Destructor
     */
    virtual ~TAsyncSSLSocketChannel();

    /**
     * Synchronously shut down the SSL channel
     *
     * @throws std::exception on failure
     */
    virtual void close();

    /**
     * Check state of channel
     * Returns true if the underlying socket is open
     */
    virtual bool good() const {
        return _socket->lowest_layer().is_open();
    }

    /**
     * Connect to the remote host
     */
    virtual void connect(const ReturnCallback& cb);

    /**
     * Asynchronously shutdown the SSL channel
     */
    virtual void shutdown(const ReturnCallback& cb);

    /**
     * Set server mode
     */
    virtual void server(bool mode) {
        BOOST_THROW_EXCEPTION(::apache::thrift::TException(
                "Unexpected call to TAsyncSSLSocketChannel::server. Server mode not yet supported"));
        //TODO (ope) - add support for server mode
        //_isServer = mode;
    }

    /**
     * Accessor to determine if channel is in server or client mode
     */
    virtual bool server() const { return _isServer; }


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
     * Construct an SSL socket channel
     */
    TAsyncSSLSocketChannel(const ::std::string& host, unsigned int port,
            ::boost::asio::io_service& ioService, ::boost::asio::ssl::context& ctx);

    /**
     * Callback handler to connect to resolved endpoint
     */
    virtual void connectToResolvedEndpoint(::boost::asio::ip::tcp::resolver::iterator epItr,
            const ReturnCallback& cb);

    /**
     * Callback handler to initiate the SSL handshake
     */
    void initiateHandshake(const ::boost::shared_ptr< ::apache::thrift::transport::TTransportException>& exceptionRtn,
            const ReturnCallback& cb);

    /**
     * Callback handler to handle asynchronous return from handshake
     */
    void handleHandshake(const ::boost::system::error_code& err, const ReturnCallback& cb);

    /**
     * Callback handler to handle asynchronous return from closing the channel
     */
    void handleShutdown(const ::boost::system::error_code& err, const ReturnCallback& cb);

private:
    bool _isServer;
    ::boost::shared_ptr< ::boost::asio::ssl::stream< ::boost::asio::ip::tcp::socket>> _socket;

    friend class TAsyncSSLSocketChannelFactory;
};


class TAsyncSSLSocketChannelFactory {
public:
    static const ::boost::asio::ssl::context::method DEFAULT_CTX_METHOD = ::boost::asio::ssl::context::tlsv1;
    //static const ::boost::asio::ssl::context::method DEFAULT_CTX_METHOD = ::boost::asio::ssl::context::sslv23;

public:

    /**
     * Construct a SSL Socket Channel Factory
     */
    TAsyncSSLSocketChannelFactory() :
        _manager(::boost::make_shared<TAsyncClientManager>()),
        _ctx(_manager->getIoService(), DEFAULT_CTX_METHOD)
    {}

    /**
     * Construct a SSL Socket Channel Factory with the specified client manager
     */
    TAsyncSSLSocketChannelFactory(const ::boost::shared_ptr<TAsyncClientManager>& manager) :
        _manager(manager),
        _ctx(_manager->getIoService(), DEFAULT_CTX_METHOD)
    {}

    /**
     * Construct a SSL Socket Channel Factory with the specified ssl context method
     */
    TAsyncSSLSocketChannelFactory(::boost::asio::ssl::context::method method) :
        _manager(::boost::make_shared<TAsyncClientManager>()),
        _ctx(_manager->getIoService(), method)
    {}

    /**
     * Construct a SSL Socket Channel Factory with the specified client manager and
     * ssl context method
     */
    TAsyncSSLSocketChannelFactory(const ::boost::shared_ptr<TAsyncClientManager>& manager,
                                  ::boost::asio::ssl::context::method method) :
        _manager(manager),
        _ctx(_manager->getIoService(), method)
    {}

    /**
     * Destructor
     */
    virtual ~TAsyncSSLSocketChannelFactory() {}

    /**
     * @returns the manager used in this socket channel factory
     */
    virtual ::boost::shared_ptr<TAsyncClientManager> getManager() const {
        return _manager;
    }

    /**
     * Create an instance of a TAsyncSSLSocketChannel to specified endpoint
     *
     * @param host  remote host to connect to
     * @oaram port  remote port to connect to
     *
     * @return initialized SSL socket channel
     */
    ::boost::shared_ptr<TAsyncSSLSocketChannel> createSocketChannel(const std::string& host,
                unsigned int port) {
        //note: we cannot make use of boost::make_shared since the constructor of TAsyncSSLSocketChannel is protected
        ::boost::shared_ptr<TAsyncSSLSocketChannel> channel(new TAsyncSSLSocketChannel(host, port, _manager->getIoService(), _ctx));
        return channel;
    }

    /**
     * Set the default SSL context options
     */
    virtual void setDefaultOptions();

    /**
     * Set ciphers to be used in SSL handshake process.
     *
     * @param ciphers  A string list of ciphers
     *
     * @throws TSSLException on failure
     */
    virtual void ciphers(const std::string& enable);

    /**
     * Enable/Disable authentication.
     *
     * @param required Require peer to present valid certificate if true
     *
     * @throws TSSLException on failure in setting mode
     */
    virtual void authenticate(bool required);

    /**
     * Load server certificate.
     *
     * @param path   Path to the certificate file
     * @param format Certificate file format
     *
     * @throws TSSLException on failure in loading certificate
     */
    virtual void loadCertificate(const std::string& path,
            ::boost::asio::ssl::context::file_format format = ::boost::asio::ssl::context::pem);

    /**
     * Load server certificate chain.
     *
     * @param path   Path to the certificate chain file
     *
     * @throws TSSLException on failure in loading certificate
     */
    virtual void loadCertificateChain(const std::string& path);

    /**
     * Load private key.
     *
     * @param path   Path to the private key file
     * @param format Private key file format
     */
    virtual void loadPrivateKey(const std::string& path,
            ::boost::asio::ssl::context::file_format format = ::boost::asio::ssl::context::pem);

    /**
     * Load trusted certificates from specified file.
     *
     * @param path Path to trusted certificate file
     */
    virtual void loadTrustedCertificates(const std::string& path);

protected:
    ::boost::shared_ptr<TAsyncClientManager> _manager;
    ::boost::asio::ssl::context _ctx;
};

}}} // namespace ::ezbake::thriftutils::tasync

#endif /* EZBAKE_THRIFTUTILS_TASYNC_TASYNCSSLSOCKETCHANNEL_H_ */
