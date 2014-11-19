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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.util.Set;

/**
 * User: jhastings
 * Date: 2/11/14
 * Time: 9:15 AM
 */
public class X509Utils {
    public enum DnFields {
        CN,
        OU,
        O,
        C
    }

    public static String getCn(LdapName name) {
        return Iterables.getFirst(getValueOfType(name, DnFields.CN), "");
    }

    public static Set<String> getOUs(LdapName name) {
        return Sets.newHashSet(getValueOfType(name, DnFields.OU));
    }

    public static String getO(LdapName name) {
        return Iterables.getFirst(getValueOfType(name, DnFields.O), "");
    }

    public static String getC(LdapName name) {
        return Iterables.getFirst(getValueOfType(name, DnFields.C), "");
    }

    private static Iterable<String> getValueOfType(final LdapName name, final DnFields type) {
        Preconditions.checkNotNull(name, "principal name must not be null");

        Set<String> values = Sets.newHashSet(FluentIterable.from(name.getRdns()).filter(new Predicate<Rdn>() {
            @Override
            public boolean apply(Rdn rdn) {
                return rdn.getType().equals(type.toString());
            }
        }).transform(new Function<Rdn, String>() {

            @Override
            public String apply(Rdn rdn) {
                return rdn.getValue().toString();
            }
        }));

        return values;
    }
}
