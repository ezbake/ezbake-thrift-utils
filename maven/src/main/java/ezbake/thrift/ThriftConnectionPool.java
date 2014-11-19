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

import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftConnectionPool<T extends TServiceClient> implements
        AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ThriftConnectionPool.class);

    private final GenericObjectPool<T> internalPool;

    /**
     * Create a new connection pool based on the supplied client and protocol factories as well as the supplied pool
     * configuration.
     *
     * @param clientFactory Factory used to create TServiceClient
     * @param protocolFactory Factory used to generate the protocol used when creating a new TServiceClient for the pool
     * @param poolConfig Configuration object used to determine things like pool evicition, max idle connections,
     *                   max active connections, etc
     */
    public ThriftConnectionPool(ClientFactory<T> clientFactory, ProtocolFactory protocolFactory,
                                GenericObjectPoolConfig poolConfig, AbandonedConfig abandonedConfig) {
        this.internalPool = new GenericObjectPool<T>(new ThriftClientFactory(
                clientFactory, protocolFactory), poolConfig, abandonedConfig);
    }

    /**
     * Retrieves an available TServiceClient from the pool. If no client is available, a new one will be created.
     *
     * @return TServiceClient from the pool.
     */
    public T getClient() {
        try {
            return internalPool.borrowObject();
        } catch (Exception e) {
            logger.error("Unable to retrieve resource from pool.", e);
            printPoolInfo();
            throw new ThriftConnectionPoolException("Error attempting to get a resource from the pool.", e);
        }
    }

    /**
     * Returns a TServiceClient to the pool. If there are already the max number of idle connections, this client
     * will simply be terminated.
     *
     * @param resource TServiceClient to be returned to the pool.
     */
    public void returnClient(T resource) {
        try {
            internalPool.returnObject(resource);
        } catch (Exception e) {
            logger.error("Error return resource to pool.", e);
            printPoolInfo();
            throw new ThriftConnectionPoolException(
                    "Could not return the resource to the pool", e);
        }
    }

    /**
     * Return a TServiceClient that is no longer valid. This will invalidate the object in the pool, thus preventing it
     * from being returned by subsequent calls to getResource(). This effectively removes the TServiceClient from the
     * pool.
     *
     * @param resource
     */
    protected void returnBrokenClient(T resource) {
        try {
            internalPool.invalidateObject(resource);
        } catch (Exception e) {
            logger.error("Error returning resource to pool.", e);
            printPoolInfo();
            throw new ThriftConnectionPoolException("Error attempting to return broken resource.", e);
        }
    }

    /**
     * Closes the ThriftConnectionPool which in turn closes and active or idle connections associated with the pool.
     */
    public void close() {
        try {
            internalPool.close();
        } catch (Exception e) {
            logger.error("Unable to close internal pool.", e);
            printPoolInfo();
            throw new ThriftConnectionPoolException("Error closing connection pool.", e);
        }
    }

    public int getActive() {
        return internalPool.getNumActive();
    }
    public int getIdle() {
        return internalPool.getNumIdle();
    }

    private void printPoolInfo() {
        if(internalPool != null && !internalPool.isClosed()) {
            logger.debug("------ POOL STATISTICS ------");
            logger.debug("Active Connections :: " + internalPool.getNumActive());
            logger.debug("Idle Connections :: " + internalPool.getNumIdle());
            logger.debug("Waiting For Connection :: " + internalPool.getNumWaiters());
            logger.debug("-----------------------------");
        }
    }
}