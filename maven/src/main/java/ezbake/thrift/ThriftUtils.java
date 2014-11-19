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

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.Properties;

import ezbakehelpers.ezconfigurationhelpers.thrift.ThriftConfigurationHelper;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TSerializer;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import ezbake.thrift.transport.EzSSLTransportFactory;
import ezbake.thrift.transport.EzSecureClientTransport;
import ezbake.thrift.transport.EzSecureServerTransport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;

/**
 * A class for some common thrift functions.
 */
public class ThriftUtils {
    /**
     * Start a thrift service for TESTING. You should be using the thrift service runner in production
     *
     * @param processor The thrift processor for the service
     * @param portNumber The port to run the service on
     */
    @VisibleForTesting
    public static TServer startSimpleServer(TProcessor processor, int portNumber) throws Exception {
        final TServerTransport transport = new TServerSocket(portNumber);
        return startSimpleServer(transport, processor);
    }

    @VisibleForTesting
    public static TServer startSslSimpleServer(TProcessor processor, int portNumber, Properties properties)
            throws Exception {
        final TServerTransport transport = getSslServerSocket(portNumber, properties);
        return startSimpleServer(transport, processor, properties);
    }

    private static TServer startSimpleServer(final TServerTransport transport, final TProcessor processor)
            throws Exception {
        return ThriftUtils.startSimpleServer(transport, processor, null);
    }

    private static TServer startSimpleServer(final TServerTransport transport, final TProcessor processor,
            Properties properties) throws Exception {

        TServer.AbstractServerArgs<?> serverArgs;
        if (properties == null) {
            serverArgs = new TServer.Args(transport).processor(processor);
        } else {
            serverArgs = ThriftUtils.getServerArgs(transport, properties).processor(processor);
        }

        final TServer server = new TSimpleServer(serverArgs);
        new Thread(new Runnable() {

            @Override
            public void run() {
                server.serve();
            }
        }).start();
        return server;
    }

    @VisibleForTesting
    public static TServer startThreadedPoolServer(TProcessor processor, int portNumber) throws Exception {
        final TServerTransport transport = new TServerSocket(portNumber);
        return startThreadedPoolServer(transport, processor);
    }

    @VisibleForTesting
    public static TServer startSslThreadedPoolServer(TProcessor processor, int portNumber,
            Properties properties) throws Exception {
        final TServerTransport transport = getSslServerSocket(portNumber, properties);
        return startThreadedPoolServer(transport, processor, properties);
    }

    private static TServer startThreadedPoolServer(final TServerTransport transport, final TProcessor processor)
            throws Exception {
        return ThriftUtils.startThreadedPoolServer(transport, processor, null);
    }

    private static TServer startThreadedPoolServer(final TServerTransport transport, final TProcessor processor,
            Properties properties) throws Exception {

        TThreadPoolServer.Args serverArgs;
        if (properties == null) {
            serverArgs = new TThreadPoolServer.Args(transport).processor(processor);
        } else {
            serverArgs =
                    (TThreadPoolServer.Args) ThriftUtils.getServerArgs(transport, properties).processor(
                            processor);
        }

        final TServer server = new TThreadPoolServer(serverArgs);
        new Thread(new Runnable() {
            @Override
            public void run() {
                server.serve();
            }
        }).start();
        return server;
    }

    @VisibleForTesting
    public static TServer startHshaServer(TProcessor processor, int portNumber) throws Exception {

        final TNonblockingServerSocket socket = new TNonblockingServerSocket(portNumber);
        final THsHaServer.Args serverArgs = new THsHaServer.Args(socket);
        serverArgs.processor(processor);
        serverArgs.inputProtocolFactory(new TCompactProtocol.Factory());
        serverArgs.outputProtocolFactory(new TCompactProtocol.Factory());
        final TServer server = new THsHaServer(serverArgs);
        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                server.serve();
            }
        });
        t.start();
        return server;
    }

    /**
     * Serialize a thrift object to binary.
     *
     * @param object The object to serialize
     * @return A byte array of the object
     * @throws TException
     */
    public static byte[] serialize(TBase<?, ?> object) throws TException {
        return new TSerializer().serialize(object);
    }

    /**
     * Serialize a thrift object to a base64-encoded string.
     *
     * @param object The object to serialize
     * @return A string of the base64-encoded serialized binary of the object
     * @throws TException
     */
    public static String serializeToBase64(TBase<?, ?> object) throws TException {
        return Base64.encodeBase64String(serialize(object));
    }

    /**
     * Deserialize a thrift object
     *
     * @param type The type of object
     * @param bytes The bytes of the object
     * @param <T> The type of object
     * @return The object
     */
    public static <T extends TBase<?, ?>> T deserialize(Class<T> type, byte[] bytes) throws TException {
        final TDeserializer deserializer = new TDeserializer();
        try {
            final T object = type.newInstance();
            deserializer.deserialize(object, bytes);
            return object;
        } catch (final Exception ex) {
            throw new TException(ex);
        }
    }

    /**
     * Deserialize a thrift object from a base64-encoded string.
     *
     * @param type The type of object
     * @param base64 The base64-encoded string of the serialization of the object
     * @param <T> The type of object
     * @return The object
     */
    public static <T extends TBase<?, ?>> T deserializeFromBase64(Class<T> type, String base64) throws TException {
        return deserialize(type, Base64.decodeBase64(base64));
    }

    public static TTransport getSslClientSocket(String host, int port, Properties properties)
            throws TTransportException {
        TTransport transport = EzSSLTransportFactory.getClientSocket(host, port, 0,
                new EzSSLTransportFactory.EzSSLTransportParameters(properties));
        return transport;
    }

    public static TServerSocket getSslServerSocket(int port, Properties properties) throws TTransportException {
        return EzSSLTransportFactory.getServerSocket(port, 0, null,
                new EzSSLTransportFactory.EzSSLTransportParameters(properties));

    }

    public static TServerSocket getSslServerSocket(InetSocketAddress addr, Properties properties)
            throws TTransportException {
        return EzSSLTransportFactory.getServerSocket(addr.getPort(), 0, addr.getAddress(),
                new EzSSLTransportFactory.EzSSLTransportParameters(properties));
    }

    @SuppressWarnings("null")
    public static TServer.AbstractServerArgs<?> getServerArgs(TServerTransport transport, Properties properties) {
        TServer.AbstractServerArgs<?> args = null;
        ThriftConfigurationHelper thriftConfiguration = new ThriftConfigurationHelper(properties);
        switch (thriftConfiguration.getServerMode()) {
            case Simple:
                args = new TServer.Args(transport);
                break;
            case ThreadedPool:
                args = new TThreadPoolServer.Args(transport);
                break;
            case HsHa:
                throw new IllegalArgumentException("Unable to create an HsHa Server Args at this time");
        }

        // Use the EzSecureTransport (exposes peer ssl certs) if using SSL
        if (thriftConfiguration.useSSL()) {
            args.inputTransportFactory(new EzSecureServerTransport.Factory(properties));
        }

        return args;
    }

    @SuppressWarnings("unchecked")
    public static <Y extends TServiceClient> Y getClient(Class<Y> clazz, HostAndPort hostAndPort,
            Properties properties) throws NoSuchMethodException, TException, Exception {
        final Constructor<?> constructor = clazz.getConstructor(TProtocol.class);
        final Object ds = constructor.newInstance(getProtocol(hostAndPort, properties));
        return (Y) ds;
    }

    @SuppressWarnings("unchecked")
    public static <Y extends TServiceClient> Y getClient(Class<Y> clazz, HostAndPort hostAndPort, String securityId,
            Properties properties) throws NoSuchMethodException, TException, Exception {
        final Constructor<?> constructor = clazz.getConstructor(TProtocol.class);
        final Object ds = constructor.newInstance(getProtocol(hostAndPort, securityId, properties));
        return (Y) ds;
    }

    @SuppressWarnings("unchecked")
    public static <Y extends TServiceClient> Y getClient(Class<Y> clazz, HostAndPort hostAndPort, String securityId,
            Properties properties, TTransportFactory transportFactory) throws NoSuchMethodException, TException,
            Exception {
        final Constructor<?> constructor = clazz.getConstructor(TProtocol.class);
        final Object ds = constructor.newInstance(getProtocol(hostAndPort, securityId, properties, transportFactory));
        return (Y) ds;
    }

    public static void quietlyClose(TServiceClient client) {
        try {
            client.getOutputProtocol().getTransport().close();
        } catch(Exception ignore) {
            //do nothing
        }
        try {
            client.getInputProtocol().getTransport().close();
        } catch(Exception ignore) {
            //do nothing
        }
    }

    protected static TProtocol getProtocol(HostAndPort hostAndPort, Properties properties) throws Exception {
        return getProtocol(hostAndPort, null, properties);
    }

    protected static TProtocol getProtocol(HostAndPort hostAndPort, String securityId, Properties properties) throws Exception {
        return getProtocol(hostAndPort, null, properties, null);
    }

    protected static TProtocol getProtocol(HostAndPort hostAndPort, String securityId, Properties properties, TTransportFactory transportFactory) throws Exception {
        TProtocol protocol;
        ThriftConfigurationHelper thriftConfiguration = new ThriftConfigurationHelper(properties);
        TTransport transport = getTransport(properties, hostAndPort, securityId, transportFactory);

        // HsHa is using framed transport with Compact protocol, but others are using binary (for now at least)
        if (thriftConfiguration.getServerMode() == ThriftConfigurationHelper.ThriftServerMode.HsHa) {
            protocol = new TCompactProtocol(transport);
        } else {
            protocol = new TBinaryProtocol(transport);
        }

        if (!transport.isOpen()) {
            transport.open();
        }

        return protocol;
    }

    protected static TTransport getTransport(Properties configuration, HostAndPort hostAndPort, String securityId, TTransportFactory transportFactory) throws TTransportException {
        TTransport transport;
        ThriftConfigurationHelper thriftConfiguration = new ThriftConfigurationHelper(configuration);

        if (thriftConfiguration.getServerMode() == ThriftConfigurationHelper.ThriftServerMode.HsHa) {
            transport = new TFramedTransport(new TSocket(hostAndPort.getHostText(), hostAndPort.getPort()));
        } else {
            if (thriftConfiguration.useSSL()) {
                transport =
                        ThriftUtils.getSslClientSocket(hostAndPort.getHostText(), hostAndPort.getPort(), configuration);
                transport = new EzSecureClientTransport(transport, configuration, securityId);
            } else {
                transport = new TSocket(hostAndPort.getHostText(), hostAndPort.getPort());
            }
        }

        // Wrap the transport using the transportFactory (if provided)
        if (transportFactory != null) {
            transport = transportFactory.getTransport(transport);
        }

        return transport;
    }

}
