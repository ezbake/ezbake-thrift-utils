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

import ezbakehelpers.ezconfigurationhelpers.ssl.SslConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.thrift.ThriftConfigurationHelper;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import ezbake.thrift.authentication.PeerSharedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 2/7/14
 * Time: 2:16 PM
 */
public abstract class EzSecureTransport extends TTransport {
    private static final Logger log = LoggerFactory.getLogger(EzSecureTransport.class);

    /* Instance members */
    protected TTransport _transport;
    protected SslConfigurationHelper sslConfiguration;
    protected boolean requireSsl;
    protected boolean negotiated = false;

    /* Constructors */
    protected EzSecureTransport(TTransport transport) {
        this(transport, new Properties());
    }
    protected EzSecureTransport(TTransport transport, Properties properties) {
        this._transport = transport;
        this.sslConfiguration = new SslConfigurationHelper(properties);
        this.requireSsl = new ThriftConfigurationHelper(properties).useSSL();
    }

    @Override
    public boolean isOpen() {
        return _transport.isOpen() && negotiated;
    }

    @Override
    public void open() throws TTransportException {
        if (negotiated) {
            throw new TTransportException("EzSecureTransport already open");
        }

        if (!_transport.isOpen()) {
            _transport.open();
        }

        try {
            SSLSocket sslSocket = (SSLSocket) ((TSocket) _transport).getSocket();
            PeerSharedData.setPeerPrincipal(sslSocket.getSession().getPeerPrincipal().getName());
            PeerSharedData.setIsThriftSecure(true);
            negotiated = true;
        } catch (SSLPeerUnverifiedException e) {
            log.warn("InvalidName ssl handshaking - peer unverified", e);
            if (sslConfiguration.isPeerAuthRequired()) {
                throw new TTransportException("SSL Peer auth is required, but was unable to verify peer: " +
                        e.getMessage(), e);
            }
        } catch (ClassCastException e) {
            log.warn("Retrieved a non-ssl transport/socket, are you sure you are listening on an SSL socket?", e);
            if (requireSsl) {
                throw new TTransportException("Could not cast underlying TSocket to an SSLSocket, but SSL is required",
                        e);
            }
        }
        log.debug("EzTransport SSL Peer CN is : {}", PeerSharedData.getPeerPrincipal().get());
    }

    @Override
    public void close() {
        PeerSharedData.cleanUp();
        _transport.close();
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        if (!isOpen()) {
            throw new TTransportException("Transport not open");
        }

        return _transport.read(buf, off, len);
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        if (!isOpen()) {
            throw new TTransportException("Transport not open");
        }

        _transport.write(buf, off, len);
    }

    /**
     * Must override flush, so calls to flush actually flush the underlying transport
     *
     * @throws TTransportException
     */
    @Override
    public void flush() throws TTransportException {
        _transport.flush();
    }
}
