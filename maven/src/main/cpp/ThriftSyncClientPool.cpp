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
 * ThriftSyncClientPool.cpp
 *
 *  Created on: Apr 23, 2014
 *      Author: oarowojolu
 */

#include <ezbake/thriftutils/ThriftSyncClientPool.h>
#include <sstream>

namespace ezbake { namespace thriftutils {

using ::ezbake::common::lrucache::LRUTimedCache;

using ::std::string;

using ::ezbake::common::HostAndPort;
using ::ezbake::ezconfiguration::EZConfiguration;
using ::ezbake::ezconfiguration::helpers::ThriftConfiguration;
using ::ezbake::ezconfiguration::helpers::ZookeeperConfiguration;
using ::ezbake::base::thrift::EzBakeBaseServiceClient;

using ::apache::thrift::transport::TSocket;
using ::apache::thrift::transport::TSSLSocketFactory;
using ::apache::thrift::transport::TFramedTransportFactory;
using ::apache::thrift::transport::TBufferedTransportFactory;
using ::apache::thrift::protocol::TBinaryProtocolFactory;
using ::apache::thrift::protocol::TCompactProtocolFactory;
using ::apache::thrift::protocol::TProtocol;
using ::apache::thrift::transport::TTransport;


ThriftSyncClientPool::ThriftSyncClientPool(const EZConfiguration& configuration,
        const string& configNamespace, uint64_t expiration) :
        ThriftClientPoolBase(configuration, configNamespace),
        _sslSocketFactory(boost::make_shared<TSSLSocketFactory>()),
        _connectionCache(ConnectionCacheType::DEFAULT_MAX_CAPACITY, expiration)
{
    //Initialize of TSSLSocketFactory. We'll use the same security configuration to create
    //SSL sockets when/if needed to generate thrift clients
    _sslSocketFactory->ciphers(_securityConfig->getSslCiphers());
    _sslSocketFactory->loadTrustedCertificates(_securityConfig->getTrustedSslCerts().c_str());
    _sslSocketFactory->loadPrivateKey(_securityConfig->getPrivateKey().c_str());
    _sslSocketFactory->loadCertificate(_securityConfig->getSslCertificate().c_str());
    _sslSocketFactory->authenticate(_securityConfig->isPeerAuthRequired());

    //initialize our ezdiscovery connection
    _serviceDiscovery.init(_zkConfig->getZookeeperConnectionString());

    //refresh list of endpoints detected from ezdiscovery
    refreshEndpoints();
    refreshCommonEndpoints();
}


ThriftSyncClientPool::~ThriftSyncClientPool() {
    this->close();
}


void ThriftSyncClientPool::clearPool() {
    {//synchronized
        ::std::lock_guard<std::recursive_mutex> lock(_m);

        _reverseLookup.erase(_reverseLookup.begin(), _reverseLookup.end());

        LRUTimedCache<string, ::boost::shared_ptr<EzBakeBaseServiceClient> >::Entry entry;
        try {
            BOOST_FOREACH(entry, _connectionCache.entrySet()) {
                entry.second->getOutputProtocol()->getTransport()->close();
                entry.second->getInputProtocol()->getTransport()->close();
            }
        } catch (...) {
            //do nothing
        }
        _connectionCache.clear();
    }
}


void ThriftSyncClientPool::close() {
    try {
        clearPool();
        _serviceCache.clear();
        _serviceDiscovery.close();
    } catch (const std::exception& e) {
        LOG4CXX_WARN(LOG, "Failed to close ThriftSyncClientPool: " << boost::diagnostic_information(e));
    }
}


void ThriftSyncClientPool::refreshEndpoints() {
    if (_applicationName.find_first_not_of(' ') != string::npos) {
        try {
            BOOST_FOREACH(const string& service, _serviceDiscovery.getServices(_applicationName)) {
                try {
                    addEndpoints(service, _serviceDiscovery.getEndpoints(_applicationName, service));
                } catch (const std::exception& e) {
                    LOG4CXX_WARN(LOG, "No " << service << " for application " << _applicationName +
                            " was found." << boost::diagnostic_information(e));
                }
            }
        } catch (const std::exception& e) {
            LOG4CXX_WARN(LOG, "Failed to get application services."
                    "  This might be okay if no application services have been registered" <<
                    boost::diagnostic_information(e));
        }
    }
}


void ThriftSyncClientPool::refreshCommonEndpoints() {
    try {
        BOOST_FOREACH(const string& service, _serviceDiscovery.getServices()) {
            try {
                addEndpoints(service, _serviceDiscovery.getEndpoints(service));
            } catch (const std::exception& e) {
                LOG4CXX_WARN(LOG, "No common " << service << " for application " << _applicationName +
                        " was found." << boost::diagnostic_information(e));
            }
        }
    } catch (const std::exception& e) {
        LOG4CXX_WARN(LOG, "Failed to get common services."
                "  This might be okay if no application services have been registered" <<
                ::boost::diagnostic_information(e));
    }
}

}} // namespace ::ezbake::thriftutils
