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
 * ThriftClientPoolBase.cpp
 *
 *  Created on: May 22, 2014
 *      Author: oarowojolu
 */

#include <ezbake/thriftutils/ThriftClientPoolBase.h>


namespace ezbake { namespace thriftutils {

using ::ezbake::common::HostAndPort;

using ::ezbake::ezconfiguration::EZConfiguration;
using ::ezbake::ezconfiguration::helpers::ApplicationConfiguration;
using ::ezbake::ezconfiguration::helpers::SecurityConfiguration;
using ::ezbake::ezconfiguration::helpers::ThriftConfiguration;
using ::ezbake::ezconfiguration::helpers::ZookeeperConfiguration;


log4cxx::LoggerPtr const ThriftClientPoolBase::LOG = log4cxx::Logger::getLogger("::ezbake::thriftutils::ThriftClientPool");


ThriftClientPoolBase::ThriftClientPoolBase(const EZConfiguration& configuration, const ::std::string& configNamespace) :
        _appConfig(ApplicationConfiguration::fromConfiguration(configuration, configNamespace)),
        _securityConfig(SecurityConfiguration::fromConfiguration(configuration, configNamespace)),
        _thriftConfig(ThriftConfiguration::fromConfiguration(configuration)),
        _zkConfig(ZookeeperConfiguration::fromConfiguration(configuration)),
        _applicationName(_appConfig->getApplicationName())
{
    if (_applicationName.find_first_not_of(' ') == ::std::string::npos) {
        LOG4CXX_WARN(LOG, "No application name was found.  Only common services will be discoverable");
    }

    if (_zkConfig->getZookeeperConnectionString().find_first_not_of(' ') == std::string::npos) {
        BOOST_THROW_EXCEPTION(::std::runtime_error(
                "No zookeeper was found.  Make sure ezbake-config.properties is on the system config path and contains " +
                ZookeeperConfiguration::ZOOKEEPER_CONNECTION_KEY));
    }
}


void ThriftClientPoolBase::addEndpoints(const ::std::string& serviceName,
                                        const ::std::vector< ::std::string>& endpoints) {
    //synchronized
    ::std::lock_guard< ::std::recursive_mutex> lock(_m);

    _serviceCache.remove(serviceName);
    BOOST_FOREACH(const ::std::string& endPoint, endpoints) {
        _serviceCache.put(serviceName, HostAndPort::fromString(endPoint));
    }
}


}} // namespace ::ezbake::thriftutils
