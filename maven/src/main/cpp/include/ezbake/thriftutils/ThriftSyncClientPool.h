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
 * ThriftSyncClientPool.h
 *
 *  Created on: Apr 7, 2014
 *      Author: oarowojolu
 */

#ifndef EZBAKE_THRIFTUTILS_THRIFTSYNCCLIENTPOOL_H_
#define EZBAKE_THRIFTUTILS_THRIFTSYNCCLIENTPOOL_H_

#include <ezbake/thriftutils/ThriftClientPoolBase.h>
#include <ezbake/ezdiscovery/ServiceDiscoverySyncClient.h>


namespace ezbake { namespace thriftutils {

class ThriftSyncClientPool : public ThriftClientPoolBase {
public:
    typedef ::ezbake::common::lrucache::LRUTimedCache< ::std::string,
                                                       ::boost::shared_ptr< ::ezbake::base::thrift::EzBakeBaseServiceClient> > ConnectionCacheType;
public:
    /**
     * Constructor/Destructor
     */
    ThriftSyncClientPool(const ::ezbake::ezconfiguration::EZConfiguration& configuration,
                     const ::std::string& configNamespace="",
                     uint64_t expiration=ConnectionCacheType::DEFAULT_CACHE_EXPIRATION);
    virtual ~ThriftSyncClientPool();

    /**
     * Removes all connections in the pool
     */
    void clearPool();

    /**
     * Release all resources, remove all thrift connections in
     * the pool and closes the connection to the Service Discovery Client
     *
     * Even though this function is called on destruction on this object,
     * it is provided as a way to clean up resources before destruction
     * when and if needed
     */
    virtual void close();

    virtual uint64_t idleClientLifetime() const {
        return _connectionCache.expiration();
    }


    /**
     * Gets a serivce client for the specified common service
     *
     * @param serviceName   service to get a client for
     *
     * @return a TServiceClient for the service
     *
     * @throws TException
     */
    template<typename T>
    ::boost::shared_ptr<T> getClient(const ::std::string& serviceName) {
        return getClient<T>(::boost::optional< ::std::string>(), serviceName);
    }

    /**
     * Gets a service client for the specified application service
     *
     * @param applicationName   application name to owning the service
     * @param serviceName       service to get a client for
     *
     * @return a TServiceClient for the service
     *
     * @throws TException
     *
     */
    template<typename T>
    ::boost::shared_ptr<T> getClient(const ::std::string& applicationName, const ::std::string& serviceName) {
        return getClient<T>(::boost::optional< ::std::string>(applicationName), serviceName);
    }

    /**
     * Return a client to the pool
     *
     * @client  the client to return
     */
    template<typename T>
    void returnToPool(::boost::shared_ptr<T>& client);


protected:
    /**
     * get client root function
     *
     * @throws TException
     */
    template<typename T>
    boost::shared_ptr<T> getClient(const boost::optional< ::std::string>& appName_opt,
                                   ::std::string serviceName);

    /**
     * @throws TException
     */
    template<typename T>
    ::boost::shared_ptr<T> createClient(const ::std::string& serviceName);

    void refreshEndpoints();

    void refreshCommonEndpoints();

private:
    ::boost::shared_ptr< ::apache::thrift::transport::TSSLSocketFactory> _sslSocketFactory;
    ConnectionCacheType _connectionCache;
    ::std::map< ::ezbake::base::thrift::EzBakeBaseServiceClient*, ::std::string> _reverseLookup;
    ::ezbake::ezdiscovery::ServiceDiscoverySyncClient _serviceDiscovery;
};

}} // namespace ::ezbake::thriftutils

//include template implementations
#include "ThriftSyncClientPool.tcc"

#endif /* EZBAKE_THRIFTUTILS_THRIFTSYNCCLIENTPOOL_H_ */
