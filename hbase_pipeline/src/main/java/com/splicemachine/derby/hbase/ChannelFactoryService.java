/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.hbase;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.pipeline.client.RpcChannelFactory;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class ChannelFactoryService{
    @SuppressWarnings("unchecked")
    public static RpcChannelFactory loadChannelFactory(SConfiguration config){
        ServiceLoader<RpcChannelFactory> serviceLoader = ServiceLoader.load(RpcChannelFactory.class);
        Iterator<RpcChannelFactory> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No ChannelFactory found!");

        RpcChannelFactory next=iter.next();
        next.configure(config);
        return next;
    }

}
