/*
 * ThriftSyncClientPool.tcc
 *
 *  Created on: Jun 3, 2014
 *      Author: oarowojolu
 */

#ifndef EZBAKE_THRIFTUTILS_THRIFTSYNCCLIENTPOOL_TCC_
#define EZBAKE_THRIFTUTILS_THRIFTSYNCCLIENTPOOL_TCC_

namespace ezbake { namespace thriftutils {

template<typename T>
boost::shared_ptr<T> ThriftSyncClientPool::getClient(const ::boost::optional< ::std::string>& appName_opt,
                                                     ::std::string serviceName) {
    //template type 'T' must inherit from 'EzBakeBaseServiceClient'
    BOOST_STATIC_ASSERT((::boost::is_base_of< ::ezbake::base::thrift::EzBakeBaseServiceClient, T>::value));

    try {
        ::std::string key = getThriftConnectionKey<T>(serviceName);

        //attempt to get from cache
        ::boost::optional< ::boost::shared_ptr< ::ezbake::base::thrift::EzBakeBaseServiceClient> > client_opt = _connectionCache.pop(key);
        if (client_opt) {
            ::boost::shared_ptr<T> client = ::boost::dynamic_pointer_cast<T>(client_opt.get());
            if (client) {
                if(!client->getOutputProtocol()->getTransport()->isOpen()) {
                    client->getOutputProtocol()->getTransport()->open();
                }
                if(!client->getInputProtocol()->getTransport()->isOpen()) {
                    client->getInputProtocol()->getTransport()->open();
                }
                return client;
            }
        }

        /*
         * Not available in cache. create new client
         */
        if (appName_opt) {
            ::std::string service = _appConfig->getApplicationServiceName(*appName_opt, serviceName);
            if (!_serviceCache.containsKey(service)) {
                std::vector< ::std::string> endpoints = _serviceDiscovery.getEndpoints(*appName_opt, serviceName);
                addEndpoints(service, endpoints);
            }
            //Now update the serviceName so getProtocol uses the right lookup
            serviceName = service;
        }

        ::boost::shared_ptr<T> client = createClient<T>(serviceName);
        _reverseLookup[client.get()] = key;
        return client;

    } catch (const ::std::exception& e) {
        BOOST_THROW_EXCEPTION(::apache::thrift::TException(::boost::diagnostic_information(e)));
    }
}


template<typename T>
::boost::shared_ptr<T> ThriftSyncClientPool::createClient(const ::std::string& serviceName) {
    unsigned int endPointCount = 0;

    for (unsigned int i = 0; i < GET_PROTOCOL_ATTEMPTS; i++) {
        {//synchronized
            ::std::lock_guard< ::std::recursive_mutex> lock(_m);

            endPointCount = _serviceCache.valueRange(serviceName);

            for (unsigned int j = 0; j < endPointCount; j++) {
                //service cache always gets the least recently used service - this serves as a load balancer
                ::boost::optional< ::ezbake::common::HostAndPort> endPoint = _serviceCache.get(serviceName);

                try {
                    ::boost::shared_ptr< ::apache::thrift::transport::TTransport> transport, socket;
                    ::boost::shared_ptr< ::apache::thrift::protocol::TProtocol> protocol;

                    if (_thriftConfig->getServerMode() == ::ezbake::ezconfiguration::helpers::ThriftConfiguration::HsHa) {
                        //use framed transport for HsHa
                        socket = ::boost::make_shared< ::apache::thrift::transport::TSocket>(endPoint->getHostText(),
                                                                                             endPoint->getPort());
                        transport = ::apache::thrift::transport::TFramedTransportFactory().getTransport(socket);
                        protocol = ::apache::thrift::protocol::TCompactProtocolFactory().getProtocol(transport);
                    } else {
                        socket = (_thriftConfig->useSSL()) ? _sslSocketFactory->createSocket(endPoint->getHostText(),
                                                                                             endPoint->getPort()) :
                                                             ::boost::make_shared< ::apache::thrift::transport::TSocket>(
                                                                                             endPoint->getHostText(),
                                                                                             endPoint->getPort());

                        transport = ::apache::thrift::transport::TBufferedTransportFactory().getTransport(socket);
                        protocol  = ::apache::thrift::protocol::TBinaryProtocolFactory().getProtocol(transport);
                    }

                    if (!transport->isOpen()) {
                        transport->open();
                    }

                    return ::boost::make_shared<T>(protocol);
                } catch (const ::std::exception &e) {
                    LOG4CXX_WARN(LOG, "Failed to connect to host(" << endPoint->toString()
                            << ") Trying next ... : " << ::boost::diagnostic_information(e));
                }
            }
        }

        refreshEndpoints();
        refreshCommonEndpoints();
    }

    ::std::ostringstream ss;
    ss << "Could not find service " << serviceName << " (found " << endPointCount << " endpoints)";
    BOOST_THROW_EXCEPTION(::std::runtime_error(ss.str()));
}

template<typename T>
void ThriftSyncClientPool::returnToPool(::boost::shared_ptr<T>& client) {
    if (client) {
        {//synchronized
            ::std::lock_guard< ::std::recursive_mutex> lock(_m);

            if (_reverseLookup.find(client.get()) != _reverseLookup.end()) {
                try {
                    //If we're using a blocking server, we need to close the connections
                    if (_thriftConfig->getServerMode().isBlocking()) {
                        client->getOutputProtocol()->getTransport()->close();
                        client->getInputProtocol()->getTransport()->close();
                    }

                    //Since we're using a size limited pool, with a short expiration, we can pool all connections
                    //including SSL connections
                    _connectionCache.put(_reverseLookup[client.get()], client);
                } catch (...) {
                    //do nothing
                }
                _reverseLookup.erase(client.get());
            }
        }
        client.reset();
    }
}


}} // namespace ::ezbake::thriftutils

#endif /* EZBAKE_THRIFTUTILS_THRIFTSYNCCLIENTPOOL_TCC_ */
