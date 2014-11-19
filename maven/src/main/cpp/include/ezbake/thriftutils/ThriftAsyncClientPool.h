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
 * ThriftAsyncClientPool.h
 *
 *  Created on: May 22, 2014
 *      Author: oarowojolu
 */

#ifndef EZBAKE_THRIFTUTILS_THRIFTASYNCCLIENTPOOL_H_
#define EZBAKE_THRIFTUTILS_THRIFTASYNCCLIENTPOOL_H_

#include <ezbake/thriftutils/ThriftClientPoolBase.h>
#include <ezbake/thriftutils/tasync/TAsyncClientManager.h>
#include <ezbake/thriftutils/tasync/TAsyncSocketChannel.h>
#include <ezbake/thriftutils/tasync/TAsyncSSLSocketChannel.h>
#include <ezbake/ezdiscovery/ServiceDiscoveryAsyncClient.h>


namespace ezbake { namespace thriftutils {

class ThriftAsyncClientPool : public ThriftClientPoolBase {
public:
    typedef ::ezbake::common::lrucache::LRUTimedCache< ::std::string,
                                                 ::boost::shared_ptr< ::ezbake::base::thrift::EzBakeBaseServiceCobClient> > ConnectionCacheType;

    typedef ::boost::function<void(void)> VoidCallback;

    typedef ::boost::function<void(const ::boost::shared_ptr< ::apache::thrift::TException>&,
                                   const ::boost::shared_ptr< ::ezbake::base::thrift::EzBakeBaseServiceCobClient>& )> GetClientCallback;


public:
    /**
     * Constructor/Destructor
     */
    ThriftAsyncClientPool(const ::ezbake::ezconfiguration::EZConfiguration& configuration,
                          const ::std::string& configNamespace="",
                          uint64_t expiration=ConnectionCacheType::DEFAULT_CACHE_EXPIRATION);
    virtual ~ThriftAsyncClientPool();

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
     * @param cb            asynchronous callback for the created client
     *                      and error notifications
     * @param serviceName   service to get a client for
     *
     */
    template<typename T>
    void getClient(const GetClientCallback& cb, const ::std::string& serviceName) {
        return getClient<T>(cb, ::boost::optional< ::std::string>(), serviceName);
    }

    /**
     * Gets a service client for the specified application service
     *
     * @param cb                asynchronous callback for the created client
     *                          and error notifications
     * @param applicationName   application name to owning the service
     * @param serviceName       service to get a client for
     *
     */
    template<typename T>
    void getClient(const GetClientCallback& cb,
            const ::std::string& applicationName, const ::std::string& serviceName) {
        return getClient<T>(cb, ::boost::optional< ::std::string>(applicationName), serviceName);
    }

    /**
     * Return a client to the pool
     *
     * @client  the client to return
     */
    template<typename T>
    void returnToPool(::boost::shared_ptr<T>& client);


protected:
    typedef struct {
        unsigned int createAttempt;
        unsigned int numEndpoints;
        unsigned int endPointIteration;
    }CreateAttemptArgs;

protected:
    /**
     * Delegate to discover services
     */
    class DiscoverServicesDelegate : public ::ezbake::ezdiscovery::ServiceDiscoveryListCallback,
                                     public ::boost::enable_shared_from_this<DiscoverServicesDelegate> {
    public:
        DiscoverServicesDelegate(ThriftAsyncClientPool& pool, const VoidCallback& cb)
            : _poolRef(pool),
              _ownerDispatch(::boost::make_shared<VoidCallback>(cb))
        {}

        virtual ~DiscoverServicesDelegate(){}

        void discoverServices();

    protected:
        void discoverCommonServices();
        virtual void process(CallbackResponse response, const std::vector<std::string>& services);

    private:
        ThriftAsyncClientPool& _poolRef;
        ::boost::shared_ptr<VoidCallback> _ownerDispatch;
        ::boost::shared_ptr<VoidCallback> _nextCb;
    };
    friend class DiscoverServicesDelegate;


    /**
     * Delegate to discover endpoints
     */
    class DiscoverEndpointsDelegate : public ::ezbake::ezdiscovery::ServiceDiscoveryListCallback,
                                      public ::boost::enable_shared_from_this<DiscoverEndpointsDelegate> {
    public:
        DiscoverEndpointsDelegate(ThriftAsyncClientPool& pool, const VoidCallback& cb,
                const ::boost::optional< ::std::string>& appName_opt, const ::std::vector< ::std::string>& services)
            : _poolRef(pool), _ownerDispatch(cb), _appName_opt(appName_opt), _servicesToDiscover(services)
        {}

        DiscoverEndpointsDelegate(ThriftAsyncClientPool& pool, const VoidCallback& cb,
                const ::boost::optional< ::std::string>& appName_opt, const ::std::string& serviceName,
                const ::std::string serviceKey)
            : _poolRef(pool), _ownerDispatch(cb), _appName_opt(appName_opt), _servicesToDiscover(1, serviceName),
              _serviceKey(serviceKey)
        {}

        virtual ~DiscoverEndpointsDelegate(){}

        void discoverEndpoints();

    protected:
        void discoverEndpoints(const ::std::string& service);
        virtual void process(CallbackResponse response, const std::vector<std::string>& endpoints);

    private:
        ThriftAsyncClientPool& _poolRef;
        VoidCallback _ownerDispatch;
        ::boost::optional< ::std::string> _appName_opt;
        ::std::vector< ::std::string> _servicesToDiscover;
        ::boost::shared_ptr<DiscoverServicesDelegate> _servicesDelegate;
        ::std::string _serviceKey; //used when adding a single serivce associated to a specific app to prevent overlaps with our app's services

        friend class DiscoverServicesDelegate;
    };
    friend class DiscoverEndpointsDelegate;


protected:
    template<typename T>
    void getClient(const GetClientCallback& cb, boost::optional< ::std::string> appName_opt, ::std::string serviceName);

    template<typename T>
    void createClient(const ::std::string& serviceName, const ::std::string& serviceCacheKey,
            const GetClientCallback& cb, unsigned int attempt=1);

    template<typename T>
    void createClientToEndpoint(const ::std::string& serviceName, const ::std::string& serviceCacheKey,
            const GetClientCallback& cb, unsigned int attempt, unsigned int numEndpoints,
            unsigned int endpointIteration=1);

    template<typename T>
    void handleCreateClientChannelConnect(const ::boost::shared_ptr< ::apache::thrift::transport::TTransportException>& err,
            const ::std::string& serviceName, const ::std::string& serviceCacheKey,
            const ::ezbake::common::HostAndPort& endpoint, CreateAttemptArgs args, const GetClientCallback& cb,
            const ::boost::shared_ptr<tasync::TAsyncCommonSocketChannel>& channel,
            const ::boost::shared_ptr< ::apache::thrift::protocol::TProtocolFactory>& protocolFactory);

    template<typename T>
    void handleAsyncChannelShutdown(const ::boost::shared_ptr<T>& client,
            const ::boost::shared_ptr< ::apache::thrift::transport::TTransportException>& err);

    void refreshAllEndpoints(const VoidCallback& cb);


private:
    ::boost::shared_ptr<tasync::TAsyncClientManager> _clientManager;
    tasync::TAsyncSocketChannelFactory _socketChanelFactory;
    tasync::TAsyncSSLSocketChannelFactory _sslSocketChanelFactory;

    ConnectionCacheType _connectionCache;
    ::std::map< ::ezbake::base::thrift::EzBakeBaseServiceCobClient*, ::std::string> _reverseLookup;

    ::ezbake::ezdiscovery::ServiceDiscoveryAsyncClient _serviceDiscovery;
};

}} // namespace ::ezbake::thriftutils

//include template implementations
#include "ThriftAsyncClientPool.tcc"

#endif /* EZBAKE_THRIFTUTILS_THRIFTASYNCCLIENTPOOL_H_ */
