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

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * User: jhastings
 * Date: 2/7/14
 * Time: 2:36 PM
 */
public class EzSecureServerTransport extends EzSecureTransport {
    private static final Logger log = LoggerFactory.getLogger(EzSecureServerTransport.class);

    protected EzSecureServerTransport(TTransport transport, Properties configuration) {
        super(transport, configuration);
    }

    public static class Factory extends TTransportFactory {
        private Properties configuration;
        public Factory(Properties configuration) {
            this.configuration = configuration;
        }

        @Override
        public TTransport getTransport(TTransport base) {
            EzSecureServerTransport ret = new EzSecureServerTransport(base, configuration);
            try {
                ret.open();
            } catch (TTransportException e) {
                log.debug("Exception creating server transport", e);
                throw new RuntimeException(e);
            }
            return ret;
        }
    }
}
