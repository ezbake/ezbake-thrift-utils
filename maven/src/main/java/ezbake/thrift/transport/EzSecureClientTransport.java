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

import com.google.common.base.Optional;
import ezbake.thrift.authentication.PeerSharedData;
import ezbake.thrift.authentication.X509Utils;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ldap.LdapName;
import java.util.Properties;

/**
 * User: jhastings
 * Date: 2/7/14
 * Time: 2:38 PM
 */
public class EzSecureClientTransport extends EzSecureTransport {
    private static final Logger log = LoggerFactory.getLogger(EzSecureClientTransport.class);

    private String targetSecurityId;

    public EzSecureClientTransport(TTransport transport, Properties configuration) throws TTransportException {
        this(transport, configuration, null);
    }

    public EzSecureClientTransport(TTransport transport, Properties configuration, String targetSecurityId) throws TTransportException {
        super(transport, configuration);
        this.targetSecurityId = targetSecurityId;
        if (validationRequired() && this.targetSecurityId == null) {
            log.warn("EzSecureClient initialized without a target security ID. This client will not verify the" +
                    "identity of its peer");
        }
    }

    @Override
    public void open() throws TTransportException {
        super.open();
        // If using SSL, check the the CN of the peer against the target security id
        if (validationRequired() && targetSecurityId != null) {
                Optional<LdapName> peerId = PeerSharedData.getPeerPrincipal();
                if (!peerId.isPresent()) {
                    throw new TTransportException("No Peer SSL Certificate present, but peer authentication is " +
                            "required");
                } else {
                    String peerCn = X509Utils.getCn(peerId.get());
                    if (!targetSecurityId.equals(peerCn)) {
                        close();
                        throw new TTransportException("SSL Certificate of server does not match security ID of " +
                                "service we are connecting to. Expected: " + targetSecurityId + ", but was: " + peerCn);
                    }
                }
        }
    }

    /**
     * Whether or not this transport should validate the peer CN
     *
     * @return true if using SSL, requiring peer cert validation
     */
    private boolean validationRequired() {
        return requireSsl && sslConfiguration.isPeerAuthRequired();
    }

}
