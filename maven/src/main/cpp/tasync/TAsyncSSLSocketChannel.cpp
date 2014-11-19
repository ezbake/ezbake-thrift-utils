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
 *
 * TAsyncSSLSocketChannel.cpp
 *  Created on: Apr 16, 2014
 *      Author: oarowojolu
 */

#include <ezbake/thriftutils/tasync/TAsyncSSLSocketChannel.h>

#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TProtocolException.h>
#include <thrift/transport/TSSLSocket.h>


namespace ezbake { namespace thriftutils { namespace tasync {

using namespace ::boost::asio;
using namespace ::boost::asio::ssl;

using ::boost::asio::ip::tcp;
using ::boost::system::error_code;

using ::apache::thrift::transport::TTransportException;
using ::apache::thrift::protocol::TProtocolException;
using ::apache::thrift::transport::TMemoryBuffer;
using ::apache::thrift::transport::TSSLException;


TAsyncSSLSocketChannel::TAsyncSSLSocketChannel(const std::string& host, unsigned int port,
        ::boost::asio::io_service& ioService, ssl::context& ctx) :
    TAsyncCommonSocketChannel(host, port, ioService),
    _isServer(false),
    _socket(::boost::make_shared< ::boost::asio::ssl::stream< ::boost::asio::ip::tcp::socket>>(ioService, ctx))
{}


TAsyncSSLSocketChannel::~TAsyncSSLSocketChannel() {
    try {
        close();
    } catch (...) {
        //ignore
    }
}


void TAsyncSSLSocketChannel::close() {
    if (good()) {
        try {
            _socket->shutdown();
            if (_socket->lowest_layer().is_open()) {
                _socket->lowest_layer().close();
            }
        } catch (const ::std::exception& e) {
            BOOST_THROW_EXCEPTION(TTransportException(
                    ::std::string("Error in closing underlying socket: ") + e.what()));
        }
    }
}


void TAsyncSSLSocketChannel::connect(const ReturnCallback& cb) {
    //Wrap connect call to handle SSL handshake after underlying socket connects
    ReturnCallback interimCob = _strand.wrap(::boost::bind(&TAsyncSSLSocketChannel::initiateHandshake,
                                                           this, _1, cb));
    TAsyncCommonSocketChannel::connect(interimCob);
}


void TAsyncSSLSocketChannel::shutdown(const ReturnCallback& cb) {
    if (good()) {
        _socket->async_shutdown(
                _strand.wrap(::boost::bind(&TAsyncSSLSocketChannel::handleShutdown,
                        this, placeholders::error, cb)));
    }
}


void TAsyncSSLSocketChannel::connectToResolvedEndpoint(tcp::resolver::iterator epItr,
        const ReturnCallback& cb) {
    tcp::endpoint endpoint = *epItr;

    if (_socket->lowest_layer().is_open()) {
        _socket->lowest_layer().close();
    }

    _socket->lowest_layer().async_connect(endpoint,
            _strand.wrap(::boost::bind(&TAsyncCommonSocketChannel::handleConnect, this,
                                       placeholders::error, ++epItr, cb)));
}


void TAsyncSSLSocketChannel::initiateHandshake(const ::boost::shared_ptr<TTransportException>& exceptionRtn,
        const ReturnCallback& cb) {
    if (!exceptionRtn) {
        //Successfully connected to host. Start SSL handshake
        ssl::stream_base::handshake_type type = (_isServer) ? ssl::stream_base::server : ssl::stream_base::client;

        _socket->async_handshake(type,
                _strand.wrap(::boost::bind(&TAsyncSSLSocketChannel::handleHandshake,
                        this, placeholders::error, cb)));
    } else {
        //error occurred. Propagate error
        cb(exceptionRtn);
    }
}


void TAsyncSSLSocketChannel::handleHandshake(const ::boost::system::error_code& err,
        const ReturnCallback& cb) {
    ::boost::shared_ptr<TTransportException> exRtn;

    if (err) {
        //SSL handshake failed, report error
        exRtn = ::boost::make_shared<TTransportException>("SSL handshake failed: " + err.message());
    }
    //else SSL handshake was successful, leave exRtn empty.

    //complete connect
    cb(exRtn);
}


void TAsyncSSLSocketChannel::handleShutdown(const ::boost::system::error_code& err,
        const ReturnCallback& cb) {
    ::boost::shared_ptr<TTransportException> exRtn;

    if (err) {
        //SSL shutdown failed, report error
        exRtn = ::boost::make_shared<TTransportException>("SSL shutdown failed: " + err.message());
    } else {
        if (_socket->lowest_layer().is_open()) {
            try {
            _socket->lowest_layer().close();
            } catch (const ::std::exception& e) {
                exRtn = ::boost::make_shared<TTransportException>(
                            ::std::string("SSL shutdown failed: Error in closing underlying socket: ") + e.what());
            }
        }
    }

    //complete shutdown response
    cb(exRtn);
}


/////

void TAsyncSSLSocketChannelFactory::setDefaultOptions() {
    _ctx.set_options(ssl::context::no_sslv2 | //disable support for SSLV2. Sets SSL_OP_NO_SSLv2 in lower layer
                     ssl::context::default_workarounds); //enable bug workarounds for compatibility. Sets SSL_OP_ALL in lower layer
}


void TAsyncSSLSocketChannelFactory::ciphers(const std::string& enable) {
    int rc = ::SSL_CTX_set_cipher_list(_ctx.impl(), enable.c_str());

    if (rc != 1) {
        BOOST_THROW_EXCEPTION(TSSLException("None of specified ciphers are supported"));
    }
}


void TAsyncSSLSocketChannelFactory::authenticate(bool required) {
    ssl::context::verify_mode mode;

    if (required) {
        mode = ssl::context::verify_peer |
               ssl::context::verify_fail_if_no_peer_cert |
               ssl::context::verify_client_once;
    } else {
        mode = ssl::context::verify_none;
    }

    error_code ec;
    _ctx.set_verify_mode(mode, ec);

    if (ec) {
        BOOST_THROW_EXCEPTION(TSSLException("System error in setting peer authentication mode: " + ec.message()));
    }
}


void TAsyncSSLSocketChannelFactory::loadCertificate(const std::string& path,
        ssl::context::file_format format) {
    error_code ec;
    _ctx.use_certificate_file(path, format, ec);

    if (ec) {
        BOOST_THROW_EXCEPTION(TSSLException("System error in loading certificate file (" + path + "): " + ec.message()));
    }
}


void TAsyncSSLSocketChannelFactory::loadCertificateChain(const std::string& path) {
    error_code ec;
    _ctx.use_certificate_chain_file(path, ec);

    if (ec) {
        BOOST_THROW_EXCEPTION(TSSLException("System error in loading certificate file (" + path + "): " + ec.message()));
    }
}


void TAsyncSSLSocketChannelFactory::loadPrivateKey(const std::string& path,
        ssl::context::file_format format) {
    error_code ec;
    _ctx.use_private_key_file(path, format, ec);

    if (ec) {
        BOOST_THROW_EXCEPTION(TSSLException("System error in loading private key file (" + path + "): " + ec.message()));
    }
}


void TAsyncSSLSocketChannelFactory::loadTrustedCertificates(const std::string& path) {
    error_code ec;
    _ctx.load_verify_file(path, ec);

    if (ec) {
        BOOST_THROW_EXCEPTION(TSSLException("System error in loading trusted certificates file (" + path + "): " + ec.message()));
    }
}


}}} // namespace ::ezbake::thriftutils::tasync
