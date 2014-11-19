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

import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.thrift.sample.SampleService;
import ezbake.thrift.sample.SampleServiceImpl;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * ThriftClientPool test class
 */
public class ThriftClientPoolTest{

	private static ThriftClientPool clientPool;
    private static ThriftServerPool serverPool;
    private static String COMMON_SERVICE = "testService";
    private static String APP_SERVICE = "testAppService";
    private static String applicationName;
    
    private final static String DEFAULT_SECURITY_ID = "DEFAULT_SEC_ID";

    @Before
    public void setup() throws Exception {
    	
    	EzConfiguration config = new EzConfiguration(new ClasspathConfigurationLoader());
        config.getProperties().put("throw.on.no.securityId", "true");
        serverPool = new ThriftServerPool(config.getProperties(), 15000);

        // start and register common service
        serverPool.startCommonService(new SampleServiceImpl(), COMMON_SERVICE, DEFAULT_SECURITY_ID);

        // start and register app specific service
        applicationName = new EzBakeApplicationConfigurationHelper(config.getProperties()).getApplicationName();
        serverPool.startApplicationService(new SampleServiceImpl(), APP_SERVICE, applicationName, DEFAULT_SECURITY_ID);

        clientPool = new ThriftClientPool(config.getProperties());
	}

    @After
    public void shutdown() throws Exception{
    	clientPool.close();
        serverPool.shutdown();
    }

    @Test
    public void testGetCommonClient() throws Exception {
        TServiceClient client = clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);
        try {
            assertNotNull(client);
        } finally {
            clientPool.returnToPool(client);
        }
    }

    @Test
    public void testGetAppClient() throws Exception {
        TServiceClient client = clientPool.getClient(APP_SERVICE, SampleService.Client.class);
        try {
            assertNotNull(client);
        } finally {
            clientPool.returnToPool(client);
        }
    }

    @Test
    public void testCommonEndpoint() throws Exception {

        SampleService.Client client = ThriftClientPoolTest.clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);

        try {
            assertEquals(30, client.add(10, 20));
        } finally {
            clientPool.returnToPool(client);
        }
    }

    @Test
    public void testAppEndpoint() throws Exception {
    	
    	SampleService.Client client = ThriftClientPoolTest.clientPool.getClient(APP_SERVICE, SampleService.Client.class);
   	  	
        try {
        	assertEquals(30, client.add(10, 20));        	  	
        }
        finally {
           clientPool.returnToPool(client);       
        }
    }
    
    @Test
    public void testAddCommonEndpoint() throws Exception {
        serverPool.startCommonService(new SampleServiceImpl(), "addedAfterTheFact", DEFAULT_SECURITY_ID);
        SampleService.Client client = clientPool.getClient("addedAfterTheFact", SampleService.Client.class);

        try {
            assertEquals(30, client.add(10, 20));
        }
        finally {
            clientPool.returnToPool(client);
        }
    }

    @Test
    public void testAddAppEndpoint() throws Exception {
        serverPool.startApplicationService(new SampleServiceImpl(), "addedAfterTheFactApp", applicationName, DEFAULT_SECURITY_ID);
        SampleService.Client client = clientPool.getClient("addedAfterTheFactApp", SampleService.Client.class);

        try {
            assertEquals(30, client.add(10, 20));
        }
        finally {
            clientPool.returnToPool(client);
        }
    }

    @Test
    public void reuseExistingClient() throws Exception {
        clientPool.clearPool();
        SampleService.Client client1 = ThriftClientPoolTest.clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);
        int hashCode1 = client1.hashCode();
        long client1Result = client1.add(10, 10);
        clientPool.returnToPool(client1);
        SampleService.Client client2 = ThriftClientPoolTest.clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);
        int hashCode2 = client2.hashCode();
        long client2Result = client2.add(10, 10);
        clientPool.returnToPool(client2);
        //Now let's make sure the calls to the service actually worked
        assertEquals(20, client1Result);
        assertEquals(20, client2Result);
    }

    @Test
    public void testTimeout() throws Exception {
        clientPool.clearPool();
        SampleService.Client client1 = ThriftClientPoolTest.clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);
        client1.add(10, 10);
        assertEquals(1, ThriftClientPoolTest.clientPool.getActive(COMMON_SERVICE + "|" + SampleService.Client.class
                .getName()));
        clientPool.returnToPool(client1);
        // Make sure we can still add even after "returning" so we can validate that we can't after an eviction
        client1.add(10, 10);
        assertEquals(0, ThriftClientPoolTest.clientPool.getActive(COMMON_SERVICE + "|" + SampleService.Client.class.getName()));
        assertEquals(1, ThriftClientPoolTest.clientPool.getIdle(COMMON_SERVICE + "|" + SampleService.Client.class.getName()));

        Thread.sleep(10000);
        try {
            client1.add(10, 10);
            fail("The connection should have closed after the timeout");
        } catch (TException ex) {
            // This is what we want
        }
    }

    @Test
    public void getNewClient() throws Exception {
        clientPool.clearPool();
        SampleService.Client client1 = ThriftClientPoolTest.clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);
        int hashCode1 = client1.hashCode();
        long client1Result = client1.add(10, 10);
        //in case we're using the Simple server, let's close the connection without returning it to the pull
        client1.getOutputProtocol().getTransport().close();

        SampleService.Client client2 = ThriftClientPoolTest.clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);
        int hashCode2 = client2.hashCode();
        long client2Result = client2.add(10, 10);
        clientPool.returnToPool(client2);
        //Since we still had hold of client1 before asking for 2, we should get a new client to the same service
        assertNotEquals(hashCode1, hashCode2);
        //Now let's make sure the calls to the service actually worked
        assertEquals(20, client1Result);
        assertEquals(20, client2Result);
    }

    @Test
    public void getAnotherAppsClient() throws Exception {
        serverPool.startApplicationService(new SampleServiceImpl(), "anotherAppService", "anotherApp", DEFAULT_SECURITY_ID);
        SampleService.Client client = clientPool.getClient("anotherApp", "anotherAppService", SampleService.Client.class);
        try {
            assertEquals(30, client.add(10, 20));
        }
        finally {
            clientPool.returnToPool(client);
        }
    }
    
    @Test
    public void testGetSecurityIdForApplication() throws Exception
    {
        final String expected = "APP_SEC_ID";
        serverPool.startApplicationService(new SampleServiceImpl(), "addedAfterTheFactApp", applicationName, expected);
        
        String securityId = clientPool.getSecurityId(applicationName);
        assertEquals(expected, securityId);
         
    }    
    
    @Test
    public void testGetSecurityForCommonService() throws Exception
    {
        assertEquals(DEFAULT_SECURITY_ID, clientPool.getSecurityId(COMMON_SERVICE));
        final String addedServiceName = "addServiceAfterTheFact";
        final String expected = "SERVICE_SEC_ID";
        serverPool.startCommonService(new SampleServiceImpl(), addedServiceName, expected);
        
        String securityId = clientPool.getSecurityId(addedServiceName); 
        assertEquals(expected, securityId);
    }

    @Test
    public void testCommonAndApp() throws Exception {
        SampleService.Client client1 = ThriftClientPoolTest.clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);
        long client1Result = client1.add(10, 10);
        assertEquals(20, client1Result);

        SampleService.Client client2 = ThriftClientPoolTest.clientPool.getClient(applicationName, APP_SERVICE, SampleService.Client.class);
        long client2Result = client2.add(10, 10);
        assertEquals(20, client2Result);

        ThriftClientPoolTest.clientPool.returnToPool(client1);
        ThriftClientPoolTest.clientPool.returnToPool(client2);

    }

    @Test
    public void returnToPool() throws Exception {
        ExecutorService es = Executors.newFixedThreadPool(25);
        List<Future<String>> futures = new ArrayList<>();
        for(int i = 0; i < 10000; i++) {
            Future<String> future = es.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    SampleService.Client client = null;
                    try {
                        client = ThriftClientPoolTest.clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);
                        client.add(10, 10);
                    } catch(TException ex) {
                        ThriftClientPoolTest.clientPool.returnBrokenToPool(client);
                        client = null;
                    } finally {
                        ThriftClientPoolTest.clientPool.returnToPool(client);
                    }
                    return null;
                }
            });
            futures.add(future);
            if (i % 3000 == 0) {
                serverPool.shutdown();
                serverPool = new ThriftServerPool(new EzConfiguration(new ClasspathConfigurationLoader()).getProperties(), 15000);
                serverPool.startCommonService(new SampleServiceImpl(), COMMON_SERVICE, DEFAULT_SECURITY_ID);
                serverPool.startApplicationService(new SampleServiceImpl(), APP_SERVICE, applicationName, DEFAULT_SECURITY_ID);
            }
        }
        es.shutdown();
        es.awaitTermination(10, TimeUnit.SECONDS);
        for(Future<String> future : futures) {
            future.get();
        }
        assertEquals(0, ThriftClientPoolTest.clientPool.getActive(COMMON_SERVICE + "|" + SampleService.Client.class.getName()));
    }
}
