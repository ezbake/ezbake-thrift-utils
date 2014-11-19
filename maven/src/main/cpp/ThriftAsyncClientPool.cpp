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
 * ThriftAsyncClientPool.cpp
 *
 *  Created on: May 22, 2014
 *      Author: oarowojolu
 */

#include <ezbake/thriftutils/ThriftAsyncClientPool.h>

namespace ezbake { namespace thriftutils {

using ::ezbake::common::lrucache::LRUTimedCache;
using ::ezbake::thriftutils::tasync::TAsyncClientManager;
using ::ezbake::thriftutils::tasync::TAsyncCommonSocketChannel;

using ::std::string;

using ::ezbake::common::HostAndPort;
using ::ezbake::ezconfiguration::EZConfiguration;
using ::ezbake::ezconfiguration::helpers::ThriftConfiguration;
using ::ezbake::ezconfiguration::helpers::ZookeeperConfiguration;
using ::ezbake::base::thrift::EzBakeBaseServiceCobClient;

using ::apache::thrift::transport::TSocket;
using ::apache::thrift::transport::TSSLSocketFactory;
using ::apache::thrift::transport::TFramedTransportFactory;
using ::apache::thrift::transport::TBufferedTransportFactory;
using ::apache::thrift::protocol::TBinaryProtocolFactory;
using ::apache::thrift::protocol::TCompactProtocolFactory;
using ::apache::thrift::protocol::TProtocol;
using ::apache::thrift::transport::TTransport;



ThriftAsyncClientPool::ThriftAsyncClientPool(const EZConfiguration& configuration,
        const string& configNamespace, uint64_t expiration) :
        ThriftClientPoolBase(configuration, configNamespace),
        _clientManager(boost::make_shared<TAsyncClientManager>()),
        _socketChanelFactory(_clientManager),
        _sslSocketChanelFactory(_clientManager),
        _connectionCache(ConnectionCacheType::DEFAULT_MAX_CAPACITY, expiration)
{
    //Initialize SSL Socket channel factory. We'll use the same security configuration to create
    //SSL sockets channels when/if needed to generate thrift clients
    _sslSocketChanelFactory.setDefaultOptions();
    _sslSocketChanelFactory.ciphers(_securityConfig->getSslCiphers());
    _sslSocketChanelFactory.loadTrustedCertificates(_securityConfig->getTrustedSslCerts());
    _sslSocketChanelFactory.loadPrivateKey(_securityConfig->getPrivateKey());
    //_sslSocketChanelFactory.loadCertificateChain(_securityConfig->getSslCertificate());
    _sslSocketChanelFactory.loadCertificate(_securityConfig->getSslCertificate());
    _sslSocketChanelFactory.authenticate(_securityConfig->isPeerAuthRequired());

    //initialize our ezdiscovery connection
    _serviceDiscovery.init(_zkConfig->getZookeeperConnectionString());
}


ThriftAsyncClientPool::~ThriftAsyncClientPool() {
    this->close();
}


void ThriftAsyncClientPool::clearPool() {
    _reverseLookup.erase(_reverseLookup.begin(), _reverseLookup.end());

    LRUTimedCache<string, ::boost::shared_ptr<EzBakeBaseServiceCobClient> >::Entry entry;
    try {
        ::boost::shared_ptr<TAsyncCommonSocketChannel> channel;
        BOOST_FOREACH(entry, _connectionCache.entrySet()) {
            channel = ::boost::dynamic_pointer_cast<TAsyncCommonSocketChannel>(entry.second->getChannel());
            channel->close();
        }
    } catch (...) {
        //do nothing
    }
    _connectionCache.clear();
}


void ThriftAsyncClientPool::close() {
    try {
        clearPool();
        _serviceCache.clear();
        _serviceDiscovery.close();
        //_clientManager->shutdown();
        _clientManager->stop();
    } catch (const std::exception& e) {
        LOG4CXX_ERROR(LOG, "Failed to close ThriftAsyncClientPool: " + boost::diagnostic_information(e));
    }
}


void ThriftAsyncClientPool::refreshAllEndpoints(const VoidCallback& cb) {
    ::boost::shared_ptr<DiscoverServicesDelegate> delegate =
            ::boost::make_shared<DiscoverServicesDelegate>(*this, cb);

    //use a shared_ptr to invoke the discoverEndpoint method as the discoverEndpoint method uses the
    //shared_ptr to hold a reference to the delegate object event after this current function
    //exits.
    delegate->discoverServices();
}


///DiscoverEndpointsDelegate
void ThriftAsyncClientPool::DiscoverEndpointsDelegate::discoverEndpoints() {
    if (_servicesToDiscover.empty()) {
        //no services to discover. Invoke return handler
        _ownerDispatch();
        return;
    }

    //start discovering end points for each service
    discoverEndpoints(_servicesToDiscover.back());
}


void ThriftAsyncClientPool::DiscoverEndpointsDelegate::discoverEndpoints(const ::std::string& service) {
    if (_appName_opt) {
        LOG4CXX_DEBUG(LOG, "discovering end points for app:" << *_appName_opt << " service:" << service);
        _poolRef._serviceDiscovery.getEndpoints(*_appName_opt, service, shared_from_this());
    } else {
        LOG4CXX_DEBUG(LOG, "discovering end points for service:" << service);
        _poolRef._serviceDiscovery.getEndpoints(service, shared_from_this());
    }
}


void ThriftAsyncClientPool::DiscoverEndpointsDelegate::process(CallbackResponse response,
        const ::std::vector< ::std::string>& endpoints) {
    if (_servicesToDiscover.empty()) {
        LOG4CXX_ERROR(_poolRef.LOG, "Error handling asynchronous callback for service discovery get endpoints."
                " Services to discover is empty on callback.");
        _ownerDispatch();
        return;
    }

    ::std::string serviceName = _servicesToDiscover.back();

    if (response != ServiceDiscoveryCallback::OK) {
        if (_appName_opt) {
            LOG4CXX_WARN(_poolRef.LOG, "No " << serviceName << " for application " << *_appName_opt << " was found.");
        } else {
            LOG4CXX_WARN(_poolRef.LOG, "No common service for " << serviceName <<  " was found.");
        }
    } else if (!endpoints.empty()) {
        if (!_serviceKey.empty()) {
            //Initiated a discovery for a service not associated with our native application.
            //Update the serviceName so we use the right lookup when checking the service cache
            serviceName = _serviceKey;
        }

        //handle end point discovery
        _poolRef.addEndpoints(serviceName, endpoints);
    }

    //remove recently discovered end point
    _servicesToDiscover.pop_back();

    if (_servicesToDiscover.empty()) {
        //done discovering all services. Trigger back to principal that dispacthed this service discovery
        _ownerDispatch();
        return;
    }

    //discover end point for next service
    discoverEndpoints(_servicesToDiscover.back());
}


///DiscoverServicesDelegate
void ThriftAsyncClientPool::DiscoverServicesDelegate::discoverServices() {
    if (_poolRef._applicationName.find_first_not_of(' ') != ::std::string::npos) {
        LOG4CXX_DEBUG(LOG, "initiating service discovery for application services");

        //discover application services
        _nextCb = ::boost::make_shared<VoidCallback>(
                ::boost::bind(&DiscoverServicesDelegate::discoverCommonServices, this));
        _poolRef._serviceDiscovery.getServices(_poolRef._applicationName, shared_from_this());
    } else {
        LOG4CXX_DEBUG(LOG, "initiating service discovery for only commmon services (no app name specified)");

        //application name not specified - only discover common services
        discoverCommonServices();
    }
}


void ThriftAsyncClientPool::DiscoverServicesDelegate::discoverCommonServices() {
    _nextCb = _ownerDispatch;
    _poolRef._serviceDiscovery.getServices(shared_from_this());
}


void ThriftAsyncClientPool::DiscoverServicesDelegate::process(CallbackResponse response,
        const ::std::vector< ::std::string>& services) {
    if ((response != ServiceDiscoveryCallback::OK) || (services.empty())) {
        LOG4CXX_WARN(_poolRef.LOG, "Failed to get application services."
                "  This might be okay if no application services have been registered");
    } else if (services.empty()) {
        //done discovering all services for this iteration. Invoke next callback
        _nextCb->operator()();
        return;
    }

    ::boost::optional< ::std::string> appName_opt;

    if ((_nextCb != _ownerDispatch) &&
        (_poolRef._applicationName.find_first_not_of(' ') != ::std::string::npos)) {
        //not looking for common serivces. If application name has been set,
        //set the optional agrument for the endpoint discovery delegate
        appName_opt = _poolRef._applicationName;
    }

    ::boost::shared_ptr<DiscoverEndpointsDelegate> delegate =
            ::boost::make_shared<DiscoverEndpointsDelegate>(_poolRef, ::boost::bind(*_nextCb),
                    appName_opt, services);

    //save a boost shared ptr to this instance inorder not to prematurely
    //destory the instance before the endpoint delegate completes
    delegate->_servicesDelegate = shared_from_this();

    //use a shared_ptr to invoke the discoverEndpoint method as the discoverEndpoint method uses the
    //shared_ptr to hold a reference to the delegate object event after this current function
    //exits.
    delegate->discoverEndpoints();
}


}} // namespace ::ezbake::thriftutils
