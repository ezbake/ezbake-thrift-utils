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
 * ThriftClientPoolBase.h
 *
 *  Created on: May 22, 2014
 *      Author: oarowojolu
 */

#ifndef EZBAKE_THRIFTUTILS_THRIFTCLIENTPOOLBASE_H_
#define EZBAKE_THRIFTUTILS_THRIFTCLIENTPOOLBASE_H_

#include <log4cxx/logger.h>

#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/foreach.hpp>
#include <boost/exception/diagnostic_information.hpp>

#include <ezbake/common/HostAndPort.h>
#include <ezbake/common/lrucache/LRUTimedCache.h>
#include <ezbake/ezconfiguration/helpers/all.h>

#include "EzBakeBaseService.h"

#include <thrift/transport/TSSLSocket.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>


namespace ezbake { namespace thriftutils {

class ThriftClientPoolBase {
public: //Constants
    static const unsigned int GET_PROTOCOL_ATTEMPTS = 2;


public:
    /**
     * Constructor/Destructor
     */
    ThriftClientPoolBase(const ::ezbake::ezconfiguration::EZConfiguration& configuration,
                         const ::std::string& configNamespace);

    virtual ~ThriftClientPoolBase(){}

    /**
     * Release all resources, remove all thrift connections in
     * the pool and closes the connection to the Service Discovery Client
     *
     * Even though this function is called on destruction on this object,
     * it is provided as a way to clean up resources before destruction
     * when and if needed
     */
    virtual void close() = 0;

    /**
     * Return the duration a client is kept in cache since it was
     * last used.
     * Any clients in the cache not used after this duration is automatically
     * expunged from the cache
     *
     * @return time duration in seconds
     */
    virtual uint64_t idleClientLifetime() const = 0;

    /*
     * Get a reference to the underlying application configuration
     *
     * @return ApplicationConfiguration shared pointer
     */
    virtual ::boost::shared_ptr< ::ezbake::ezconfiguration::helpers::ApplicationConfiguration> getAppConfiguration() const {
        return _appConfig;
    }

    /*
     * Get a reference to the underlying security configuration
     *
     * @return SecurityConfiguration shared pointer
     */
    virtual ::boost::shared_ptr< ::ezbake::ezconfiguration::helpers::SecurityConfiguration> getSecurityConfiguration() const {
        return _securityConfig;
    }

    /*
     * Get a reference to the underlying thrift configuration
     *
     * @return ThriftConfiguration shared pointer
     */
    virtual ::boost::shared_ptr< ::ezbake::ezconfiguration::helpers::ThriftConfiguration> getThriftConfiguration() const {
        return _thriftConfig;
    }

    /*
     * Get a reference to the underlying zookeepr configuration
     *
     * @return ZookeeperConfiguration shared pointer
     */
    virtual ::boost::shared_ptr< ::ezbake::ezconfiguration::helpers::ZookeeperConfiguration> getZookeeperConfiguration() const {
        return _zkConfig;
    }

protected:
    void addEndpoints(const ::std::string& serviceName, const std::vector< ::std::string>& endpoints);

    template<typename T>
    ::std::string getThriftConnectionKey(const ::std::string& serviceName) {
        return serviceName + "|" + typeid(T).name();
    }

protected:
    static log4cxx::LoggerPtr const LOG;
    ::std::recursive_mutex _m;

    ::boost::shared_ptr< ::ezbake::ezconfiguration::helpers::ApplicationConfiguration> _appConfig;
    ::boost::shared_ptr< ::ezbake::ezconfiguration::helpers::SecurityConfiguration> _securityConfig;
    ::boost::shared_ptr< ::ezbake::ezconfiguration::helpers::ThriftConfiguration> _thriftConfig;
    ::boost::shared_ptr< ::ezbake::ezconfiguration::helpers::ZookeeperConfiguration> _zkConfig;

    ::std::string _applicationName;

    ::ezbake::common::lrucache::LRUCache< ::std::string, ::ezbake::common::HostAndPort> _serviceCache;
};

}} // namespace ::ezbake::thriftutils

#endif /* EZBAKE_THRIFTUTILS_THRIFTCLIENTPOOLBASE_H_ */
