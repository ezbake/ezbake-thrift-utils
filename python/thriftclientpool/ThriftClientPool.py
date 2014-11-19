#   Copyright (C) 2013-2014 Computer Sciences Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import logging
from kazoo import client
import ezdiscovery
from ezconfiguration.EZConfiguration import EZConfiguration
from ezconfiguration.helpers import ApplicationConfiguration
from ezconfiguration.helpers import SecurityConfiguration
from ezconfiguration.helpers import ThriftConfiguration
import threading

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift import Thrift

class ThriftClientPool:

    def __init__(self, configuration, shouldConnectToZooKeeper=True):

        if configuration is None or not isinstance(configuration, EZConfiguration):
            raise Exception("Invalid configuration.")

        self.__ezConfiguration = configuration
        self.__applicationName = ApplicationConfiguration.fromConfiguration(configuration).getApplicationName()
        self.__securityConfiguration = SecurityConfiguration.fromConfiguration(configuration)
        self.__thriftConfiguration = ThriftConfiguration.fromConfiguration(configuration)

        # TODO: Add logging stuff here                
        logging.basicConfig(filename="/tmp/py.log", level=logging.DEBUG, format='%(asctime)s \t%(levelname)s: \t%(message)s\t')
        self.__log = logging.getLogger(__name__)

        self.__serviceMapLock = threading.RLock()
        self.__serviceMap = {}
        self.__connectionPool = {}
        self.__reverseLookup = {}

        print "Application name: " + self.__applicationName
        if self.__applicationName is None:
            self.__log.warn("No application name was found. Only common services will be discoverable.")
        
        # zookeeper stuff
        if shouldConnectToZooKeeper:
            ezdiscovery.connect('localhost:2181')

        try:
            self.__common_services = list(ezdiscovery.get_common_services())
        except Exception:
            self.__log.error("Unable to get commone services")
            raise

        self.__refreshEndPoints()
        self.__refreshCommonEndPoints()

    def __refreshEndPoints(self):
        if self.__applicationName is not None:
            try:
                for service in ezdiscovery.get_services(self.__applicationName):
                    try:
                        endPoints = ezdiscovery.get_endpoints(self.__applicationName, service)
                        self._addEndpoints(service, endPoints)
                    except Exception as e:
                        self.__log.warn("No " + service + " for application " + self.__applicationName + " was found")
            except Exception as e:
                self.__log.warn("Failed to get application services. This might be okay if the application hasn't registered any services.")

    def __refreshCommonEndPoints(self):
        try:
            for service in ezdiscovery.get_common_services():
                try:
                    endPoints = ezdiscovery.get_common_endpoints(service)
                    self._addEndpoints(service, endPoints)
                except Exception as e:
                    self.__log.warn("No common service " + service + " was found.")
        except Exception as e:
            self.__log.warn("Failed to get common services. This might be okay if no common service has been defined.")
                    
                        
    def _addEndpoints(self, service, endPoints):
        with self.__serviceMapLock:
            if service in self.__serviceMap:
                del self.__serviceMap[service]
            self.__serviceMap[service] = []
            for endPoint in endPoints:
                # TODO: Should we verify the URL isn't malformed?
                self.__serviceMap[service].append(endPoint)
        
    def getClient(self, applicationName, serviceName, clazz):

        try:
            key = self.__getThriftConnectionKey(serviceName, clazz)
            if key in self.__connectionPool:
                client = self.__connectionPool[key].poll()
                if not client.getOutputProtocol().getTransport().isOpen():
                    client.getOutputProtocol().getTransport().open()
                if not client.getInputProtocol().getTransport().isOpen():
                    client.getInputProtocol().getTransport().open()
                    
                return client
            if applicationName is not None:
                raise Exception("You didn't implement this.")
        
            

        except Exception as e:
            raise Thrift.TException(str(e))
            


    def __getThriftConnectionKey(self, serviceName, clientClass):
        # TODO: I'm not sure that this is what we want to do here
        return serviceName + "|" + str(clientClass)

    def __isCommonService(self, serviceName):

        if serviceName in self.__common_services:
            return True

        if ezdiscovery.is_service_common(serviceName):
            self.__common_services.append(serviceName)
            return True
        return False
        

