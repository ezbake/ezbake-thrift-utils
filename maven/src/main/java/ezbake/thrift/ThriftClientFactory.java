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

import ezbake.base.thrift.EzBakeBaseService;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftClientFactory<T extends TServiceClient> extends BasePooledObjectFactory<T> {

    private static final Logger logger = LoggerFactory.getLogger(ThriftClientFactory.class);

    private ClientFactory<T> clientFactory;
    private ProtocolFactory protocolFactory;

    public ThriftClientFactory(ClientFactory<T> clientFactory,
                               ProtocolFactory protocolFactory) {
        this.clientFactory = clientFactory;
        this.protocolFactory = protocolFactory;
    }

    @Override
    public T create() throws Exception {
        try {
            TProtocol protocol = protocolFactory.create();
            return clientFactory.create(protocol);
        } catch (Exception e) {
            logger.error("Error creating new service client for pool.");
            throw e;
        }
    }

    @Override
    public PooledObject<T> wrap(T t) {
        return new DefaultPooledObject<T>(t);
    }

    @Override
    public boolean validateObject(PooledObject<T> p) {
        System.out.println("Validating connection....");
        try {
            return ((EzBakeBaseService.Client) p.getObject()).ping() && super.validateObject(p);
        } catch (TException e) {
            return false;
        }
    }

    @Override
    public void destroyObject(PooledObject<T> obj) {
        ThriftUtils.quietlyClose(obj.getObject());
    }
}
