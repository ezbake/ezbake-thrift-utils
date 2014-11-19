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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import com.google.common.net.HostAndPort;
import ezbake.common.properties.EzProperties;
import ezbake.ezdiscovery.ServiceDiscoveryClient;
import ezbake.ezdiscovery.ServiceDiscovery;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.thrift.ThriftConfigurationHelper;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class handles service discovery, client creation, client pooling, and connection creation
 * for all thrift services.  When connecting to a common service or one of your application services
 * all that is required is the service name.  This class will find the endpoint, initiate the conenction,
 * including setting up SSL if necessary, and then pool that connection for future use.
 */
public class ThriftClientPool {
    private static final Logger logger = LoggerFactory.getLogger(ThriftClientPool.class);

    private final EzProperties configuration;
    private final ThriftConfigurationHelper thriftConfiguration;
    private final GenericObjectPoolConfig poolConfig;
    private final AbandonedConfig abandonedConfig;
    private final String applicationName;
    private final String applicationSecurityId;

    private final Multimap<String, HostAndPort> serviceMap = Multimaps.synchronizedMultimap(
            ArrayListMultimap.<String, HostAndPort>create());
    private final Map<String, ThriftConnectionPool<TServiceClient>> connectionPool = new ConcurrentHashMap<>();

    //we need a reverse lookup from the client back to the pool.  Using a list because the Hash of the
    //TServiceClient wasn't reliably returning unique values so we're doing straight object equality now
    private final List<ClientPoolKey> reverseLookup = new ArrayList<>();

    // A cache to hold the security ids
    private final ConcurrentMap<String, String> securityIdCache = new MapMaker().makeMap();

    // A list of all the common service names
    private Set<String> commonServices= Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());


    /**
     * Initializes a new ThriftClientPool.
     *
     * @param configuration The properties for this instance of the pool
     */
    public ThriftClientPool(Properties configuration) {
        this.configuration = new EzProperties(configuration, true);
        /* We only need to use this to get the application name, so we want this to fall out of scope and get garbage
         collected */
        EzBakeApplicationConfigurationHelper appConfig = new EzBakeApplicationConfigurationHelper(configuration);
        this.applicationName = appConfig.getApplicationName();
        this.applicationSecurityId = appConfig.getSecurityID();
        if (Strings.isNullOrEmpty(applicationName)) {
            logger.warn("No application name was found.  Only common services will be discoverable");
        }

        this.thriftConfiguration = new ThriftConfigurationHelper(configuration);

        poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(thriftConfiguration.getMaxIdleClients());
        poolConfig.setMaxTotal(thriftConfiguration.getMaxPoolClients());
        poolConfig.setMinEvictableIdleTimeMillis(thriftConfiguration.getMillisIdleBeforeEviction());
        poolConfig.setTimeBetweenEvictionRunsMillis(thriftConfiguration.getMillisIdleBeforeEviction());
        poolConfig.setTestOnBorrow(thriftConfiguration.getTestOnBorrow());
        poolConfig.setTestWhileIdle(thriftConfiguration.getTestWhileIdle());
        poolConfig.setBlockWhenExhausted(thriftConfiguration.getBlockWhenExhausted());

        abandonedConfig = new AbandonedConfig();
        abandonedConfig.setLogAbandoned(thriftConfiguration.getLogAbandoned());
        abandonedConfig.setRemoveAbandonedOnBorrow(thriftConfiguration.getRemoveAbandonedOnBorrow());
        abandonedConfig.setRemoveAbandonedOnMaintenance(thriftConfiguration.getRemoveAbandonedOnMaintenance());
        abandonedConfig.setRemoveAbandonedTimeout(thriftConfiguration.getRemoveAbandonedTimeout());

        if(!thriftConfiguration.actuallyPoolClients()) {
            logger.info("Thrift client pool is NOT actually pooling clients!");
        }

        ServiceDiscovery serviceDiscoveryClient = getServiceDiscoveryClient();
        try {
            this.commonServices.addAll(serviceDiscoveryClient.getServices());
        } catch(Exception e) {
            logger.warn("Could not obtain common services!");
            throw new RuntimeException(e);
        }

        RefreshEndpoints(serviceDiscoveryClient);
        RefreshCommonEndpoints(serviceDiscoveryClient);
        closeClient(serviceDiscoveryClient);
    }

    protected void RefreshEndpoints(ServiceDiscovery serviceDiscoveryClient) {
        if (!Strings.isNullOrEmpty(applicationName)) {
            try {
                for (String service : serviceDiscoveryClient.getServices(applicationName)) {
                    try {
                        List<String> endPoints = serviceDiscoveryClient.getEndpoints(applicationName, service);
                        AddEndpoints(service, endPoints);
                    } catch (Exception ex) {
                        logger.warn("No " + service + "for application " + applicationName + " was found.", ex);
                        // do nothing, try next
                    }
                }
            } catch(Exception ex) {
                logger.warn("Failed to get application services.  This might be okay if no application services have been registered", ex);
            }
        }
    }

    protected void RefreshCommonEndpoints(ServiceDiscovery serviceDiscoveryClient) {
        try {
            for (String service : serviceDiscoveryClient.getServices()) {
                try {
                    List<String> endPoints = serviceDiscoveryClient.getEndpoints(service);
                    AddEndpoints(service, endPoints);
                } catch (Exception ex) {
                    // do nothing, try next
                    logger.warn("No common " + service + "for application " + applicationName + " was found.", ex);
                }
            }
        } catch(Exception ex) {
            logger.warn("Failed to get common services.  This might be okay if no common services have been registered", ex);
        }
    }

    protected void AddEndpoints(String service, List<String> endPoints) {
        synchronized (serviceMap) {
            serviceMap.removeAll(service);
            for (String endPoint : endPoints) {
                try {
                    HostAndPort hostAndPort = HostAndPort.fromString(endPoint);
                    serviceMap.put(service, hostAndPort);
                } catch (Exception ex) {
                    logger.warn("Failed to connect to host(" + endPoint + ") Trying next...", ex);
                }
            }
        }
    }

    /**
     * Gets a TServiceClient to the thrift service
     *
     * @param serviceName The name of the service you are looking for
     * @param clazz   The type of the client you want to return
     * @param <Y>     The client
     */
    @SuppressWarnings("unchecked")
    public <Y extends TServiceClient> Y getClient(String serviceName, Class<Y> clazz) throws TException {
        return getClient(null, serviceName, clazz);
    }

    /**
     * Gets a TServiceClient to the thrift service that is being exposed by another application
     *
     * @param applicationName The application that exposed the service
     * @param serviceName The name of the service you are looking for
     * @param clazz   The type of the client you want to return
     * @param <Y>     The client
     */
    @SuppressWarnings("unchecked")
    public <Y extends TServiceClient> Y getClient(final String applicationName, String serviceName, final Class<Y> clazz) throws TException {

        try {
            String key = getThriftConnectionKey(serviceName, clazz);
            if (thriftConfiguration.actuallyPoolClients() && connectionPool.containsKey(key)) {
                Y resource = (Y) connectionPool.get(key).getClient();
                addReverseLookup(resource, key);
                return resource;
            }

            if (applicationName != null) {
                //Let's prefix the service name with the application name to prevent overlaps with our app's services
                String service = EzBakeApplicationConfigurationHelper.getApplicationServiceName(applicationName, serviceName);
                if (!serviceMap.containsKey(service)) {
                    ServiceDiscovery serviceDiscoveryClient = getServiceDiscoveryClient();
                    List<String> endPoints = serviceDiscoveryClient.getEndpoints(applicationName, serviceName);
                    closeClient(serviceDiscoveryClient);
                    AddEndpoints(service, endPoints);
                }
                //Now update the serviceName so getProtocol uses the right lookup
                serviceName = service;
            }

            final String poolServiceName = serviceName; //need a final variable for use in an inner class

            if(!thriftConfiguration.actuallyPoolClients()) {
                Constructor constructor = clazz.getConstructor(TProtocol.class);
                Object ds = constructor.newInstance(getProtocol(applicationName, serviceName, 1));
                return (Y) ds;
            }

            final ThriftConnectionPool<TServiceClient> pool = new ThriftConnectionPool<>(
                new ClientFactory<TServiceClient>() {
                    @Override
                    public Y create(TProtocol tProtocol) throws Exception{
                        Constructor constructor = clazz.getConstructor(TProtocol.class);
                        Object serviceClient = constructor.newInstance(tProtocol);
                        return (Y)serviceClient;
                    }
                }, new ProtocolFactory() {
                    public TProtocol create() throws Exception {
                        return getProtocol(applicationName, poolServiceName, 1);
                    }
            }, poolConfig, abandonedConfig);

            connectionPool.put(key, pool);

            Y resource = (Y) pool.getClient();
            addReverseLookup(resource, key);
            return resource;
            //return (Y) ds;
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }


    /**
     * Returns the client to the pool
     *
     * @param client The client that you no longer want to use
     */
    public void returnToPool(TServiceClient client) {
        returnToPool(client, false);
    }

    public void returnBrokenToPool(TServiceClient client) {
        returnToPool(client, true);
    }

    private void returnToPool(TServiceClient client, boolean broken) {
        if(!thriftConfiguration.actuallyPoolClients()) {
            ThriftUtils.quietlyClose(client);
        }
        String key = getReverseLookup(client);
        if (client != null && key != null) {
            try {
                // Deal with simple server not really being a pool
                if(thriftConfiguration.getServerMode().isBlocking() || broken) {
                    ThriftUtils.quietlyClose(client);
                    connectionPool.get(key).returnBrokenClient(client);
                } else {
                    connectionPool.get(key).returnClient(client);
                }
            } catch (Exception e) {
                // close since the object isn't going back to the pool
                ThriftUtils.quietlyClose(client);
                logger.warn("Didn't actually return to pool", e);
            }
        } else if (client != null) {
            // close since the object isn't going back to the pool
            ThriftUtils.quietlyClose(client);
            logger.warn("Didn't find the client key in the lookup. Nothing returned to pool");
        }
    }


    @VisibleForTesting
    protected int getActive(String key) {
        return connectionPool.get(key).getActive();
    }

    @VisibleForTesting
    protected int getIdle(String key) {
        return connectionPool.get(key).getIdle();
    }

    public synchronized void clearPool() {
        reverseLookup.clear();
        for(Entry<String, ThriftConnectionPool<TServiceClient>> entry : connectionPool.entrySet()) {
            entry.getValue().close();
        }
        connectionPool.clear();
    }

    /**
     * For a given common service or application retrieve its security id from zookeeper.
     *
     * @param applicationOrCommonServiceName the name of the application or common service to get the id for
     *
     * @return the security id for the application/common service name or null if it could not be found
     * */
    public String getSecurityId(String applicationOrCommonServiceName) {
        String name = Preconditions.checkNotNull(applicationOrCommonServiceName);
        if(name.isEmpty()) {
            return null;
        }

        String securityId = securityIdCache.get(name);
        if(securityId != null) {
            return securityId;
        }

        ServiceDiscovery serviceDiscoveryClient = getServiceDiscoveryClient();
        try {
            if(isCommonService(name, serviceDiscoveryClient)) {
                securityId = serviceDiscoveryClient.getSecurityIdForCommonService(name);
            } else {
                securityId = serviceDiscoveryClient.getSecurityIdForApplication(name);
            }
        } catch(Exception e) {
            logger.error("Could not find security id for " + name, e);
            if (configuration.getBoolean("throw.on.no.securityId", false)) {
                throw new RuntimeException("Could not find security id for " + name);
            }
            return null;
        } finally {
            closeClient(serviceDiscoveryClient);
        }

        // cache our value so we don't have to go to zookeeper every time
        if(securityId != null)
            securityIdCache.put(name, securityId);

        return securityId;
    }

    private TProtocol getProtocol(String applicationName, String serviceName, int attempt) throws Exception {
        int endpointCount;
        StringBuilder exceptionList = new StringBuilder();
        synchronized (serviceMap) {

            Collection<HostAndPort> endPoints = serviceMap.get(serviceName);
            List<HostAndPort> list= Lists.newArrayList(endPoints);
            if (endPoints.size() >1)
            {
                Collections.shuffle(list);   // distributes load on endpoints
            }
            endpointCount = endPoints.size();
            for (HostAndPort hostAndPort : list) {
                try {
                    final String securityId;
                    if (applicationName != null) {
                        //Getting another app's security id
                        securityId = getSecurityId(applicationName);
                    } else if (commonServices.contains(serviceName)) { //isCommonService reconnects to zookeeper, don't need that here
                        //Getting a common service's security id
                        securityId = getSecurityId(serviceName);
                    } else {
                        //Use your own app's security id
                        securityId = applicationSecurityId;
                    }
                    return ThriftUtils.getProtocol(hostAndPort, securityId, configuration);
                } catch (Exception ex) {
                    exceptionList.append("\nHost: ");
                    exceptionList.append(hostAndPort.toString());
                    exceptionList.append(" Exception: ");
                    exceptionList.append(ex.getMessage());
                    logger.warn("Failed to connect to host(" + hostAndPort.toString() + ") Trying next...", ex);
                }
            }
        }
        if (attempt == 1) {
            ServiceDiscovery serviceDiscoveryClient = getServiceDiscoveryClient();
            RefreshEndpoints(serviceDiscoveryClient);
            RefreshCommonEndpoints(serviceDiscoveryClient);
            closeClient(serviceDiscoveryClient);
            return getProtocol(applicationName, serviceName, 2);
        }
        throw new RuntimeException("Could not connect to service " + serviceName + " (found " + endpointCount + " endpoints)" +
            exceptionList.toString());
    }

    private void addReverseLookup(TServiceClient client, String key) {
        synchronized (reverseLookup) {
            reverseLookup.add(new ClientPoolKey(client, key));
        }
    }

    /**
     * If the reverse lookup contains the client, it will remove it from the lookup and return the key
     * @param client The TServiceClient
     * @return The key, if the client exists, otherwise null
     */
    private String getReverseLookup(TServiceClient client) {
        if (client == null) {
            return null;
        }
        int foundIndex = -1;
        String key = null;
        synchronized (reverseLookup) {
            for(int i = 0; i < reverseLookup.size(); i++) {
                if (reverseLookup.get(i).client.equals(client)) {
                    foundIndex = i;
                    break;
                }
            }
            if (foundIndex >= 0) {
                key = reverseLookup.get(foundIndex).key;
            }
        }
        return key;
    }

    public void close() {
        clearPool();
        synchronized (serviceMap) {
            serviceMap.clear();
        }
    }

    private String getThriftConnectionKey(String serviceName, Class<?> clientClass) {
        return serviceName + "|" + clientClass.getName();
    }

    private boolean isCommonService(String serviceName, ServiceDiscovery serviceDiscoveryClient) throws Exception {

        if(commonServices.contains(serviceName)) {
            return true;
        }
        boolean isCommon = serviceDiscoveryClient.isServiceCommon(serviceName);

        if (isCommon)
        {
            commonServices.add(serviceName);
        }
        return isCommon;

    }

    private ServiceDiscoveryClient getServiceDiscoveryClient() {
        return new ServiceDiscoveryClient(configuration);
    }

    private void closeClient(ServiceDiscovery client) {
        try {
            client.close();
        } catch(IOException ignored) {
            //don't care
        }
    }

    private static class ClientPoolKey {
        public ClientPoolKey(TServiceClient client, String key) {
            this.client = client;
            this.key = key;
        }
        public TServiceClient client;
        public String key;
    }
}
