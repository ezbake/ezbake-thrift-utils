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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

/**
 * User: jhastings
 * Date: 2/7/14
 * Time: 2:33 PM
 */
public class PeerSharedData {
    private static final Logger logger = LoggerFactory.getLogger(PeerSharedData.class);

    private static ThreadLocal<Boolean> secureThrift = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };
    private static ThreadLocal<Optional<LdapName>> peerPrincipal = new ThreadLocal<Optional<LdapName>>() {
        @Override
        protected Optional<LdapName> initialValue() {
            return Optional.absent();
        }
    };

    private PeerSharedData() {}

    public static void cleanUp() {
        clearIsThriftSecure();
        clearPeerPrincipal();
    }

    public static void setIsThriftSecure(boolean secure) {
        secureThrift.set(secure);
    }
    public static boolean isThriftSecure() {
        return secureThrift.get();
    }
    public static void clearIsThriftSecure() {
        secureThrift.set(false);
    }

    public static void setPeerPrincipal(String dn) {
        try {
            peerPrincipal.set(Optional.fromNullable(new LdapName(dn)));
        } catch (InvalidNameException e) {
            logger.warn("Unable to parse peer DN. PeerPrincipal will be unavailable");
        }
    }

    public static Optional<LdapName> getPeerPrincipal() {
        return peerPrincipal.get();
    }

    public static void clearPeerPrincipal() {
        peerPrincipal.set(Optional.<LdapName>absent());
    }

}
