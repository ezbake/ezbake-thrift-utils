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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.security.test.MockEzSecurityToken;

import java.util.*;

/**
 * Test Utilities related to interacting with Thrift and the Ezbake libraries
 */
public class ThriftTestUtils {
    @VisibleForTesting
    /**
     * Create a security token for testing.  Since this isn't actually signed, it won't work in production but
     * should pass unit test
     */
    public static EzSecurityToken generateTestSecurityToken(String... auths) {
        //For backwards compatibility
        return generateTestSecurityToken("DatasetsTest", "DatasetsTest", Lists.newArrayList(auths));
    }

    public static EzSecurityToken generateTestSecurityToken(String securityId, String targetSecurityId, List<String> auths) {
        // Projects/Groups
        Map<String, List<String>> projects = new HashMap<String, List<String>>();
        projects.put("EzBake", Arrays.asList("EzBakePlatform"));
        // Token
        EzSecurityToken token = MockEzSecurityToken.getBlankToken(securityId, targetSecurityId, 100000l);
        MockEzSecurityToken.populateExternalProjectGroups(token, projects, false);
        MockEzSecurityToken.populateAuthorizations(token, null, new HashSet<String>(auths));
        MockEzSecurityToken.populateUserInfo(token, "Doe, John", null, null);
        token.getValidity().setIssuer("EzSecurity");
        return token;
    }


}
