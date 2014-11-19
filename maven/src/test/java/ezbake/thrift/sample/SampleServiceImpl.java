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

package ezbake.thrift.sample;

import ezbake.base.thrift.EzBakeBaseThriftService;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;

/**
 * Dumb test service
 */
public class SampleServiceImpl extends EzBakeBaseThriftService implements SampleService.Iface {
    @Override
    public TProcessor getThriftProcessor() {
        return new SampleService.Processor(this);
    }

    @Override
    public long add(int i1, int i2) throws TException {
        return i1 + i2;
    }
}
