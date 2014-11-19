/*
 * ThriftAsyncClientPool.tcc
 *
 *  Created on: Jun 3, 2014
 *      Author: oarowojolu
 */

#ifndef EZBAKE_THRIFTUTILS_THRIFTASYNCCLIENTPOOL_TCC_
#define EZBAKE_THRIFTUTILS_THRIFTASYNCCLIENTPOOL_TCC_

namespace ezbake { namespace thriftutils {

template<typename T>
void ThriftAsyncClientPool::returnToPool(::boost::shared_ptr<T>& client) {
    if (client) {
        if (_reverseLookup.find(client.get()) != _reverseLookup.end()) {
            ::boost::shared_ptr<T> clientToReturn;
            clientToReturn.swap(client); //release shared pointer ownership

            _reverseLookup.erase(clientToReturn.get());

            try {
                //If we're using a blocking server, we need to close the connections
                if (_thriftConfig->getServerMode().isBlocking()) {
                    if (_thriftConfig->useSSL()) {
                        ::boost::shared_ptr<tasync::TAsyncSSLSocketChannel> channel =
                                ::boost::dynamic_pointer_cast<tasync::TAsyncSSLSocketChannel>(clientToReturn->getChannel());

                        //dispatch async call to shutdown SSL channel
                        channel->shutdown(::boost::bind(&ThriftAsyncClientPool::handleAsyncChannelShutdown<T>,
                                                        this, clientToReturn, _1));
                        return;
                    }

                    ::boost::shared_ptr<tasync::TAsyncCommonSocketChannel> channel =
                            ::boost::dynamic_pointer_cast<tasync::TAsyncCommonSocketChannel>(clientToReturn->getChannel());
                    channel->close();
                }

                //Since we're using a size limited pool, with a short expiration, we can pool all connections
                //including SSL connections
                _connectionCache.put(_reverseLookup[clientToReturn.get()], clientToReturn);
            } catch (...) {
                //do nothing
            }
        }
    }
}


template<typename T>
void ThriftAsyncClientPool::getClient(const GetClientCallback& cb,
        ::boost::optional< ::std::string> appName_opt, ::std::string serviceName) {
    //template type 'T' must inherit from 'EzBakeBaseServiceCobClient'
    BOOST_STATIC_ASSERT((::boost::is_base_of< ::ezbake::base::thrift::EzBakeBaseServiceCobClient, T>::value));

    try {
        ::std::string key = getThriftConnectionKey<T>(serviceName);

        //attempt to get from cache
        ::boost::optional< ::boost::shared_ptr< ::ezbake::base::thrift::EzBakeBaseServiceCobClient> > client_opt = _connectionCache.pop(key);
        if (client_opt) {
            ::boost::shared_ptr<T> client = ::boost::dynamic_pointer_cast<T>(client_opt.get());
            if (client) {
                ::boost::shared_ptr< ::apache::thrift::TException> err;
                ::boost::shared_ptr<tasync::TAsyncCommonSocketChannel> channel =
                        ::boost::dynamic_pointer_cast<tasync::TAsyncCommonSocketChannel>(client->getChannel());

                if(!channel->good()) {
                    //reconnect channel
                    channel->connect(::boost::bind(cb, _1, client));
                } else {
                    //channel is good. Call return callback
                    cb(err, client);
                }

                //done with getting client from cache
                return;
            }
        }

        /*
         * Not available in cache. create new client
         */
        ::std::string serviceCacheKey = serviceName;
        if (appName_opt) {
            serviceCacheKey = _appConfig->getApplicationServiceName(*appName_opt, serviceName);
            if (!_serviceCache.containsKey(serviceCacheKey)) {
                ::boost::shared_ptr<DiscoverEndpointsDelegate> delegate =
                        ::boost::make_shared<DiscoverEndpointsDelegate>(*this,
                                ::boost::bind(&ThriftAsyncClientPool::createClient<T>, this,
                                        serviceName, serviceCacheKey, cb, 1),
                                appName_opt, serviceName, serviceCacheKey);
                delegate->discoverEndpoints();
                return;
            }
        }

        createClient<T>(serviceName, serviceCacheKey, cb, 1);
    } catch (const ::std::exception& e) {
        BOOST_THROW_EXCEPTION(::apache::thrift::TException(::boost::diagnostic_information(e)));
    }
}


template<typename T>
void ThriftAsyncClientPool::createClient(const ::std::string& serviceName,
        const ::std::string& serviceCacheKey, const GetClientCallback& cb, unsigned int attempt) {

    unsigned int numEndpoints = _serviceCache.valueRange(serviceCacheKey);

    if (numEndpoints > 0) {
        createClientToEndpoint<T>(serviceName, serviceCacheKey, cb, attempt, numEndpoints);
    } else if (attempt <= GET_PROTOCOL_ATTEMPTS) {
        /*
         * No endpoints available in our service discovery cache.
         * Attempt to create client after refreshing the discovery cache
         * to see if new services are discoverable.
         */
        refreshAllEndpoints(::boost::bind(&ThriftAsyncClientPool::createClient<T>,
                this, serviceName, serviceCacheKey, cb, ++attempt));
        return;
    } else {
        ::boost::shared_ptr< ::apache::thrift::TException> ex;

        try {
            //use try..catch to wrap exception with diagnostic information (location of exception)
            BOOST_THROW_EXCEPTION(::std::runtime_error("Could not get an endpoint for service " + serviceName));
        } catch (const ::std::exception &e ){
            ex = ::boost::make_shared< ::apache::thrift::TException>(::boost::diagnostic_information(e));
        }
        cb(ex, ::boost::shared_ptr<T>());
    }
}


template<typename T>
void ThriftAsyncClientPool::createClientToEndpoint(const ::std::string& serviceName, const ::std::string& serviceCacheKey,
        const GetClientCallback& cb, unsigned int createAttempt, unsigned int numEndpoints, unsigned int endpointIteration) {

    //service cache always gets the least recently used service - this serves as a load balancer
    ::boost::optional< ::ezbake::common::HostAndPort> endPoint = _serviceCache.get(serviceCacheKey);

    if (endPoint) {
        ::boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory> protocolFactory;
        ::boost::shared_ptr<tasync::TAsyncCommonSocketChannel> channel;

        if (_thriftConfig->useSSL()) {
            channel = _sslSocketChanelFactory.createSocketChannel(endPoint->getHostText(), endPoint->getPort());
        } else {
            channel = _socketChanelFactory.createSocketChannel(endPoint->getHostText(), endPoint->getPort());
        }

        if (_thriftConfig->getServerMode() == ::ezbake::ezconfiguration::helpers::ThriftConfiguration::HsHa) {
            protocolFactory = ::boost::make_shared< ::apache::thrift::protocol::TCompactProtocolFactory>();
        } else {
            protocolFactory  = ::boost::make_shared< ::apache::thrift::protocol::TBinaryProtocolFactory>();
        }

        if (!channel->good()) {
            CreateAttemptArgs args = {createAttempt, endpointIteration, numEndpoints};
            channel->connect(::boost::bind(&ThriftAsyncClientPool::handleCreateClientChannelConnect<T>,
                    this, _1, serviceName, serviceCacheKey, *endPoint, args, cb, channel, protocolFactory));
        } else {
            //Channel is good. Create Cob client with channel
            ::boost::shared_ptr< ::apache::thrift::TException> err;
            ::boost::shared_ptr<T> client = ::boost::make_shared<T>(channel, protocolFactory.get());
            cb(err, client);
        }
    } else {
        ::boost::shared_ptr< ::apache::thrift::TException> ex;

        try {
            //use try..catch to wrap exception with diagnostic information (location of exception)
            ::std::ostringstream ss;
            ss << "Could not get endpoint for service " << serviceName << " (@iteration " << endpointIteration
                    << ". Found " << numEndpoints << " endpoints)";
            BOOST_THROW_EXCEPTION(::std::runtime_error(ss.str()));
        } catch (const ::std::exception &e ){
            ex = ::boost::make_shared< ::apache::thrift::TException>(::boost::diagnostic_information(e));
        }
        cb(ex, ::boost::shared_ptr<T>());
    }
}


template<typename T>
void ThriftAsyncClientPool::handleCreateClientChannelConnect(const ::boost::shared_ptr< ::apache::thrift::transport::TTransportException>& err,
        const ::std::string& serviceName, const ::std::string& serviceCacheKey, const ::ezbake::common::HostAndPort& endpoint,
        CreateAttemptArgs args, const GetClientCallback& cb,
        const ::boost::shared_ptr<tasync::TAsyncCommonSocketChannel>& channel,
        const ::boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory>& protocolFactory) {
    if (err) {
        LOG4CXX_WARN(LOG, "Failed to connect to host(" << endpoint.toString() << "): " << err->what());

        if (args.endPointIteration < args.numEndpoints) {
            //Connection to endpoint failed. Try next endpoint in service discovery cache.
            LOG4CXX_INFO(LOG, "Retrying another endpoint in service discovery cache");
            createClientToEndpoint<T>(serviceName, serviceCacheKey, cb, args.createAttempt,
                    args.numEndpoints, args.endPointIteration+1);
        } else if (args.createAttempt <= GET_PROTOCOL_ATTEMPTS) {
            /*
             * We've max'd all endpoints we have in our service discovery cache.
             * Refresh the discovery cache to see if new services are discoverable.
             */
            LOG4CXX_INFO(LOG, "Retrying another endpoint after refreshing service discovery cache");
            refreshAllEndpoints(::boost::bind(&ThriftAsyncClientPool::createClient<T>,
                    this, serviceName, serviceCacheKey, cb, args.createAttempt+1));
        } else {
            ::boost::shared_ptr< ::apache::thrift::TException> ex;

            try {
                //use try..catch to wrap exception with diagnostic information (location of exception)
                ::std::ostringstream ss;
                ss << "Could not find service " << serviceName << " (found " << args.numEndpoints << " endpoints)";
                BOOST_THROW_EXCEPTION(::std::runtime_error(ss.str()));
            } catch (const ::std::exception &e ){
                ex = ::boost::make_shared< ::apache::thrift::TException>(::boost::diagnostic_information(e));
            }
            cb(ex, ::boost::shared_ptr<T>());
        }
        return;

    } else {
        //create Cob client with channel
        ::boost::shared_ptr< ::apache::thrift::TException> ex;
        ::boost::shared_ptr<T> client;

        try {
            if (channel->good()) {
                client = ::boost::make_shared<T>(channel, protocolFactory.get());
                _reverseLookup[client.get()] = getThriftConnectionKey<T>(serviceName);
            } else {
                BOOST_THROW_EXCEPTION(::apache::thrift::TException("Async channel connect in invalid state"));
            }
        } catch (const ::std::exception &e ){
            ex = ::boost::make_shared< ::apache::thrift::TException>(::boost::diagnostic_information(e));
        }

        cb(ex, client);
        return;
    }
}


template<typename T>
void ThriftAsyncClientPool::handleAsyncChannelShutdown(const ::boost::shared_ptr<T>& client,
        const ::boost::shared_ptr< ::apache::thrift::transport::TTransportException>& err) {
    if (err) {
        //No need to add client to our connection cache since it failed at shutting down
        //the SSL channel.
        LOG4CXX_WARN(LOG, "Failed to shutdown SSL channel for client: " << err->what());
    } else {
        //Since we're using a size limited pool, with a short expiration, we can pool all connections
        //including SSL connections
        _connectionCache.put(_reverseLookup[client.get()], client);
    }
}


}} // namespace ::ezbake::thriftutils

#endif /* EZBAKE_THRIFTUTILS_THRIFTASYNCCLIENTPOOL_TCC_ */
