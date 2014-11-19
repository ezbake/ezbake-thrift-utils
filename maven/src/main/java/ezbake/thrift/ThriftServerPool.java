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

package ezbake.thrift;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.local.zookeeper.LocalZookeeper;
import ezbakehelpers.ezconfigurationhelpers.thrift.ThriftConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.zookeeper.ZookeeperConfigurationHelper;
import org.apache.thrift.server.TServer;
import com.google.common.net.HostAndPort;
import ezbake.ezdiscovery.ServiceDiscoveryClient;
import ezbake.ezdiscovery.ServiceDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.*;

/**
 * This class is solely meant to be used for Unit tests.  It's a mechanism to run thrift services in process
 */
@VisibleForTesting
public class ThriftServerPool {
    private static final Logger logger = LoggerFactory.getLogger(ThriftServerPool.class);
    private final List<TServer> thriftServers = new ArrayList<>();
    private int portNumber;
    private LocalZookeeper zookeeper;
    private ServiceDiscovery discovery;
    private Properties properties;
    private ThriftConfigurationHelper thriftConfiguration;

    /**
     * Create a server pool
     * @param startingPortNumber The port number to start services on.  Each service will start incrementing
     *                           this number by 1
     */
    public ThriftServerPool(Properties properties, int startingPortNumber) {
        portNumber = startingPortNumber;
        this.properties = properties;
        thriftConfiguration = new ThriftConfigurationHelper(properties);
        ZookeeperConfigurationHelper zooConfig = new ZookeeperConfigurationHelper(properties);
        if (Strings.isNullOrEmpty(zooConfig.getZookeeperConnectionString())) {
            throw new RuntimeException("No zookeeper connection string was found.");
        }
        try {
            zookeeper = new LocalZookeeper(Integer.parseInt(zooConfig.getZookeeperConnectionString().split(":")[1]));
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get valid port from Zookeeper configuration", ex);
        }
        discovery = new ServiceDiscoveryClient(zooConfig.getZookeeperConnectionString());
    }

    /**
     * Shutdown all the running servers
     */
    public void shutdown() {
        for (TServer server : thriftServers) {
            server.stop();
        }
        try {
            zookeeper.shutdown();
            discovery.close();
        } catch (IOException ex) {
            //don't care
        }
    }

    /**
     * Starts a local server
     * @param service The service to start
     * @param serviceName The name of the service that we are starting
     */
    public void startCommonService(EzBakeBaseThriftService service, String serviceName, String securityId) throws Exception {
        startService(service, serviceName, null, securityId);
    }

    public void startApplicationService(EzBakeBaseThriftService service, String serviceName, String applicationName, String securityId) throws Exception    {
        checkNotNull(applicationName);
        startService(service, serviceName, applicationName, securityId);
    }

    private void startService(EzBakeBaseThriftService service, String serviceName, String applicationName, String securityId) throws Exception {
        checkNotNull(service);
        checkNotNull(serviceName);

        HostAndPort hostAndPort = HostAndPort.fromParts("localhost", portNumber);
        if (applicationName == null) {
            discovery.registerEndpoint(serviceName, hostAndPort.toString());
            discovery.setSecurityIdForCommonService(serviceName, securityId);
        } else {
            discovery.registerEndpoint(applicationName, serviceName, hostAndPort.toString());
            discovery.setSecurityIdForApplication(applicationName, securityId);
        }

        // Give the service it's own EzConfiguration with the correct security ID
        properties.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, securityId);
        service.setConfigurationProperties(properties);

        TServer server;
        switch (thriftConfiguration.getServerMode()) {
            case Simple:
                if(thriftConfiguration.useSSL()) {
                    server = ThriftUtils.startSslSimpleServer(service.getThriftProcessor(), portNumber++,
                            properties);
                } else {
                    server = ThriftUtils.startSimpleServer(service.getThriftProcessor(),  portNumber++);
                }
                break;
            case HsHa:
                if(thriftConfiguration.useSSL()) {
                    logger.warn("ThriftUtils based HsHa doesn't currently support SSL.");
                    throw new RuntimeException("Unsupported server mode. (HsHa with SSL)");
                }
                server = ThriftUtils.startHshaServer(service.getThriftProcessor(),  portNumber++);
                break;
            case ThreadedPool:
                if(thriftConfiguration.useSSL()) {
                    server = ThriftUtils.startSslThreadedPoolServer(service.getThriftProcessor(),  portNumber++,
                            properties);
                } else {
                    server = ThriftUtils.startThreadedPoolServer(service.getThriftProcessor(),  portNumber++);
                }
                break;
            default:
                throw new RuntimeException("Unrecognized server mode");
        }

        thriftServers.add(server);
    }
}
