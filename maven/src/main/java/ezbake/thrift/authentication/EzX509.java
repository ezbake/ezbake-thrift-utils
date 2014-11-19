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

package ezbake.thrift.authentication;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import javax.naming.ldap.LdapName;

/**
 * User: jhastings
 * Date: 2/7/14
 * Time: 2:39 PM
 */
public class EzX509 {
    private Optional<LdapName> peerDn;

    public EzX509() {
        peerDn = PeerSharedData.getPeerPrincipal();
    }

    public boolean isValidPeer() {
        return peerDn.isPresent();
    }

    public void assertValidPeer(String msg) {
        Preconditions.checkState(peerDn.isPresent(), msg);
    }

    public void assertValidPeer(String msg, Object ... msgArgs) {
        Preconditions.checkState(peerDn.isPresent(), msg, msgArgs);
    }

    public String getPeerSecurityID() throws ThriftPeerUnavailableException {
        if (!peerDn.isPresent()) {
            throw new ThriftPeerUnavailableException("Peer DN is not present");
        }
        return X509Utils.getCn(peerDn.get());
    }
}
