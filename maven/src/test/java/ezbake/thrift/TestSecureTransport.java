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

import com.google.common.net.HostAndPort;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.thrift.sample.SampleService;
import ezbake.thrift.sample.SampleServiceImpl;
import org.apache.thrift.TException;
import org.junit.*;
import ezbake.thrift.authentication.EzX509;
import ezbake.thrift.authentication.ThriftPeerUnavailableException;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 2/10/14
 * Time: 10:23 AM
 */
public class TestSecureTransport {

    private final static String DEFAULT_SECURITY_ID = "ThriftUtilsServer";
    private static final int startingPort = 35000;

    private static ThriftServerPool serverPool;
    private static ThriftClientPool clientPool;
    private static String COMMON_SERVICE = "testService";
    private static EzConfiguration ezConfiguration;
    private static Properties serverProperties;
    private static Properties clientProperties;

    @BeforeClass
    public static void setUpEzConfiguration() throws EzConfigurationLoaderException {
        ezConfiguration = new EzConfiguration(new ClasspathConfigurationLoader());

        // Force SSL true
        Properties properties = ezConfiguration.getProperties();
        properties.setProperty(EzBakePropertyConstants.THRIFT_USE_SSL, String.valueOf(true));
        // Server needs to use the "server" security ID
        serverProperties = new EzProperties(properties, true);
        serverProperties.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY,
                TestSecureTransport.class.getResource("/ssl/server").getFile());
        // Client needs to use the "client" security ID
        clientProperties = new EzProperties(properties, true);
        clientProperties.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, "client");
        clientProperties.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY,
                TestSecureTransport.class.getResource("/ssl/client").getFile());
    }

    @Before
    public void setup() throws Exception {
        // Start the service
        serverPool = new ThriftServerPool(serverProperties, startingPort);
        serverPool.startCommonService(new SampleServiceImpl(), COMMON_SERVICE, "ThriftUtilsServer");

        // Initialize the client pool
        clientPool = new ThriftClientPool(clientProperties);
    }

    @After
    public void shutdown() throws Exception {
        clientPool.close();
        serverPool.shutdown();
    }

    @Test
    public void testUtilsGetClient() throws Exception {
        SampleService.Client client = ThriftUtils.getClient(SampleService.Client.class, HostAndPort.fromParts("localhost", startingPort), clientProperties);
        client.add(1, 3);

        EzX509 auth = new EzX509();
        auth.assertValidPeer("Must have SSL peer");
        Assert.assertEquals("ThriftUtilsServer", auth.getPeerSecurityID());
    }

    @Test
    public void testPoolGetClient() throws TException, ThriftPeerUnavailableException {
        SampleService.Client client = clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);
        long res = client.add(1, 3);

        EzX509 auth = new EzX509();
        auth.assertValidPeer("Must have SSL peer");
        Assert.assertEquals("ThriftUtilsServer", auth.getPeerSecurityID());

        clientPool.returnToPool(client);

        SampleService.Client client2 = clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);
        long res2 = client2.add(1, 3);
        clientPool.returnToPool(client2);

        Assert.assertEquals(res, res2);

        SampleService.Client client3 = clientPool.getClient(COMMON_SERVICE, SampleService.Client.class);
        long res3 = client3.add(1, 3);
        clientPool.returnToPool(client3);

        Assert.assertEquals(res2, res3);

    }

}
