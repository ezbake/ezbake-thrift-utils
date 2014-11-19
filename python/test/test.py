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

from thriftclientpool.ThriftClientPool import ThriftClientPool
from ezconfiguration.EZConfiguration import EZConfiguration
from ezconfiguration.helpers import ApplicationConfiguration
from kazoo import testing
import os
import getpass
import unittest
import ezdiscovery

from kazoo import testing

class ThriftControlPoolTest(testing.KazooTestCase):

    def setUp(self):
        super(ThriftControlPoolTest, self).setUp()
        #print ezdiscovery.zk
        ezdiscovery.zk = self.client

        config = EZConfiguration()
        applicationName = ApplicationConfiguration.fromConfiguration(config).getApplicationName()

        ezdiscovery.register_common_endpoint('common_service_one', 'localhost', 8080)
        ezdiscovery.register_common_endpoint('common_service_two', 'localhost', 8081)
        ezdiscovery.register_common_endpoint('common_service_three', 'localhost', 8082)
        ezdiscovery.register_common_endpoint('common_service_multi', '192.168.1.1', 6060 )
        ezdiscovery.register_common_endpoint('common_service_multi', '192.168.1.2', 6161)
        
        ezdiscovery.register_endpoint(applicationName, "service_one", 'localhost', 8083)
        ezdiscovery.register_endpoint(applicationName, "service_two", 'localhost', 8084)
        ezdiscovery.register_endpoint(applicationName, "service_three", 'localhost', 8085)

        ezdiscovery.register_endpoint("NotThriftClientPool", "unknown_service_three", 'localhost', 8085)
        ezdiscovery.register_endpoint("NotThriftClientPool", "unknown_service_three", 'localhost', 8085)
        ezdiscovery.register_endpoint("NotThriftClientPool", "unknown_service_three", 'localhost', 8085)



        self.cp = ThriftClientPool(config, shouldConnectToZooKeeper = False)



    def test_endpoints(self):
        self.assertTrue( "common_service_one" in self.cp._ThriftClientPool__serviceMap)
        self.assertTrue( "common_service_two" in self.cp._ThriftClientPool__serviceMap)
        self.assertTrue( "common_service_three" in self.cp._ThriftClientPool__serviceMap)
        self.assertTrue( "service_one" in self.cp._ThriftClientPool__serviceMap)
        self.assertTrue( "service_two" in self.cp._ThriftClientPool__serviceMap)
        self.assertTrue( "service_three" in self.cp._ThriftClientPool__serviceMap)
        self.assertFalse( "unknown_service_one" in self.cp._ThriftClientPool__serviceMap)
        self.assertFalse( "unknown_service_two" in self.cp._ThriftClientPool__serviceMap)
        self.assertFalse( "unknown_service_three" in self.cp._ThriftClientPool__serviceMap)

        self.assertTrue( "common_service_multi" in self.cp._ThriftClientPool__serviceMap)
        self.assertEqual(len(self.cp._ThriftClientPool__serviceMap["common_service_multi"]), 2)


#    def test_disconnect(self):
#        self.cp.getClient("ThriftClientPool", "service_one", ThriftClientPool)

if __name__ == '__main__':
    # You may need to modify this environment variable to locate Zookeeper.
    if 'EZCONFIGURATION_DIR' not in os.environ:
        os.environ['EZCONFIGURATION_DIR'] = '.'
    if 'ZOOKEEPER_PATH' not in os.environ:
        os.environ['ZOOKEEPER_PATH'] = os.sep + os.path.join(
            'home',
            getpass.getuser(),
            '.ez_dev',
            'zookeeper'
        )
    unittest.main()
