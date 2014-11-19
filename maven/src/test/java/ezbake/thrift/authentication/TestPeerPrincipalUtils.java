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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import java.util.Set;

/**
 * User: jhastings
 * Date: 2/11/14
 * Time: 10:07 AM
 */
public class TestPeerPrincipalUtils {
    private static final String dn = "CN=Last First, OU=CSC, O=U.S. Government, C=US";
    private static final String dnOneOU = dn;
    private static final String dnMultiOU = "CN=Last First, OU=CSC, OU=People, O=U.S. Government, C=US";

    @Test
    public void testGetCn() throws InvalidNameException {
        LdapName name = new LdapName(dn);

        String cn = X509Utils.getCn(name);
        Assert.assertEquals("Last First", cn);
    }

    @Test(expected=NullPointerException.class)
    public void testGetCnBlank() {
        LdapName name = null;
        String cn = X509Utils.getCn(name);
    }

    @Test
    public void getOuSingular() throws InvalidNameException {
        LdapName name = new LdapName(dnOneOU);
        Set<String> ous = X509Utils.getOUs(name);
        Assert.assertTrue(Iterables.size(ous) == 1);
        Assert.assertTrue(ImmutableSet.of("CSC").containsAll(ous));
    }

    @Test
    public void getOuMultiple() throws InvalidNameException {
        LdapName name = new LdapName(dnMultiOU);
        Set<String> ous = X509Utils.getOUs(name);
        Assert.assertTrue(Iterables.size(ous) > 1);
        Assert.assertTrue(ImmutableSet.of("CSC", "People").containsAll(ous));
    }

    @Test
    public void getO() throws InvalidNameException {
        LdapName name = new LdapName(dn);
        String o = X509Utils.getO(name);
        Assert.assertNotNull(o);
        Assert.assertNotEquals("", o);
        Assert.assertEquals("U.S. Government", o);
    }

    @Test
    public void getC() throws InvalidNameException {
        LdapName name = new LdapName(dn);
        String c = X509Utils.getC(name);
        Assert.assertNotNull(c);
        Assert.assertEquals("US", c);
    }
}
