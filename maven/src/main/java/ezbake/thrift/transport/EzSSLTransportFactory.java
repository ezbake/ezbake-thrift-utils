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

package ezbake.thrift.transport;

import ezbake.crypto.utils.EzSSL;
import ezbakehelpers.ezconfigurationhelpers.ssl.SslConfigurationHelper;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.security.KeyStore;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 7/7/14
 * Time: 9:44 AM
 */
public class EzSSLTransportFactory {

    /**
     * Create an SSL TServerSocket for the given parameters
     *
     * @param port port server should listen on
     * @param clientTimeout client socket timeout (seconds)
     * @param ifAddress interface to listen on
     * @param params parameters - includes key/trust stores
     * @return a TServerSocket that has been initialized
     * @throws TTransportException
     */
    public static TServerSocket getServerSocket(int port, int clientTimeout, InetAddress ifAddress, EzSSLTransportParameters params) throws TTransportException {
        if (params == null) {
            throw new TTransportException("EzSSLTransportParameters must not be null");
        }

        SSLContext ctx = createSSLContext(params);
        return createServer(ctx.getServerSocketFactory(), port, clientTimeout, params.clientAuth, ifAddress, params);
    }

    private static TServerSocket createServer(SSLServerSocketFactory factory, int port, int timeout, boolean clientAuth,
                                              InetAddress ifAddress, EzSSLTransportParameters params)
            throws TTransportException
    {
        try {
            SSLServerSocket serverSocket = (SSLServerSocket) factory.createServerSocket(port, 100, ifAddress);
            serverSocket.setSoTimeout(timeout);
            serverSocket.setNeedClientAuth(clientAuth);
            if (params != null && params.cipherSuites != null) {
                serverSocket.setEnabledCipherSuites(params.cipherSuites);
            }
            return new TServerSocket(serverSocket);
        } catch (Exception e) {
            throw new TTransportException("Could not bind to port " + port, e);
        }
    }

    /**
     * Create an SSL TSocket for client connections
     * @param host host to connect to
     * @param port port to connect to
     * @param timeout socket timeout
     * @param params parameters - includes key/trust stores
     * @return an open SSL TSocket
     * @throws TTransportException
     */
    public static TSocket getClientSocket(String host, int port, int timeout, EzSSLTransportParameters params) throws TTransportException {
        if (params == null) {
            throw new TTransportException("Either one of the KeyStore or TrustStore must be set for SSLTransportParameters");
        }

        SSLContext ctx = createSSLContext(params);
        return createClient(ctx.getSocketFactory(), host, port, timeout);
    }

    private static TSocket createClient(SSLSocketFactory factory, String host, int port, int timeout) throws TTransportException {
        try {
            SSLSocket socket = (SSLSocket) factory.createSocket(host, port);
            socket.setSoTimeout(timeout);
            return new TSocket(socket);
        } catch (Exception e) {
            throw new TTransportException("Could not connect to " + host + " on port " + port, e);
        }
    }

    private static SSLContext createSSLContext(EzSSLTransportParameters params) throws TTransportException {
        SSLContext ctx;
        try {
            ctx = SSLContext.getInstance(params.protocol);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(params.trustManagerType);
            KeyStore ts = KeyStore.getInstance(params.trustStoreType);

            InputStream tsis = EzSSL.getISFromFileOrClasspath(params.properties, params.trustStore);
            ts.load(tsis, params.trustPass.toCharArray());
            tmf.init(ts);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(params.keyManagerType);
            KeyStore ks = KeyStore.getInstance(params.keyStoreType);

            InputStream ksis = EzSSL.getISFromFileOrClasspath(params.properties, params.keyStore);
            ks.load(ksis, params.keyPass.toCharArray());
            kmf.init(ks, params.keyPass.toCharArray());

            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        } catch (IOException e) {
            throw new TTransportException("Error creating the transport: unable to load keystores", e);
        } catch (Exception e) {
            throw new TTransportException("Error creating the transport", e);
        }
        return ctx;
    }

    public static class EzSSLTransportParameters {
        protected Properties properties;
        protected SslConfigurationHelper sslHelper;
        protected String protocol = "TLS";

        protected String keyStore;
        protected String keyPass;
        protected String keyManagerType = KeyManagerFactory.getDefaultAlgorithm();
        protected String keyStoreType = "JKS";

        protected String trustStore;
        protected String trustPass;
        protected String trustManagerType = TrustManagerFactory.getDefaultAlgorithm();
        protected String trustStoreType = "JKS";

        protected String[] cipherSuites = null;
        protected boolean clientAuth = false;

        public EzSSLTransportParameters(Properties properties) {
            this.properties = properties;
            sslHelper = new SslConfigurationHelper(properties);
            String protocol = sslHelper.getSslProtocol();
            String[] ciphers = sslHelper.getSslCiphers();

            if (protocol != null) {
                this.protocol = protocol;
            }
            if (ciphers != null && ciphers.length != 0) {
                this.cipherSuites = ciphers;
            }
            this.clientAuth = sslHelper.isPeerAuthRequired();

            this.keyStore = sslHelper.getKeystoreFile();
            this.keyPass = sslHelper.getKeystorePass();
            this.keyStoreType = sslHelper.getKeystoreType();

            this.trustStore = sslHelper.getTruststoreFile();
            this.trustPass = sslHelper.getTruststorePass();
            this.trustStoreType = sslHelper.getTruststoreType();
        }
    }
}
