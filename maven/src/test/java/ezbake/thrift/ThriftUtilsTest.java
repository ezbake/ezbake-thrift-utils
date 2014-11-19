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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.thrift.sample.SampleService;
import ezbake.thrift.sample.SampleServiceImpl;
import ezbake.thrift.sample.SampleStruct;
import org.apache.thrift.TServiceClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.net.HostAndPort;

import ezbake.configuration.EzConfiguration;

/**
 * ThriftUtils test class
 */
public class ThriftUtilsTest {

    private static ThriftServerPool serverPool;
    private static String COMMON_SERVICE = "testService";
    private static int portNum = 14000;
    private final static String DEFAULT_SECURITY_ID = "DEFAULT_SEC_ID";
    private static EzConfiguration config;

    @Before
    public void setup() throws Exception {

        config = new EzConfiguration(new ClasspathConfigurationLoader());
        serverPool = new ThriftServerPool(config.getProperties(), 14000);

        // start and register service
        serverPool.startCommonService(new SampleServiceImpl(), COMMON_SERVICE, DEFAULT_SECURITY_ID);

    }

    @After
    public void shutdown() throws Exception {

        serverPool.shutdown();
    }

    @Test
    public void testGetClient() throws Exception {
        final HostAndPort hostandPort = HostAndPort.fromParts("localhost", portNum);
        final TServiceClient client = ThriftUtils.getClient(SampleService.Client.class, hostandPort,
                config.getProperties());
        try {
            assertNotNull(client);

        } finally {
            client.getInputProtocol().getTransport().close();
        }
    }

    @Test
    public void testSerializeDeserialize() throws Exception {
        final SampleStruct myStruct = new SampleStruct(10, "Hello");

        final byte[] buffer = ThriftUtils.serialize(myStruct);
        assertNotNull(buffer);

        final SampleStruct newStruct = ThriftUtils.deserialize(SampleStruct.class, buffer);
        assertEquals(10, newStruct.getMyInt());
        assertEquals("Hello", newStruct.getMyString());
    }

    @Test
    public void testSerializeDeserializeBase64() throws Exception {
        final SampleStruct myStruct = new SampleStruct(10, "Hello");

        final String base64 = ThriftUtils.serializeToBase64(myStruct);
        assertNotNull(base64);

        final SampleStruct newStruct = ThriftUtils.deserializeFromBase64(SampleStruct.class, base64);
        assertEquals(10, newStruct.getMyInt());
        assertEquals("Hello", newStruct.getMyString());
    }
}
