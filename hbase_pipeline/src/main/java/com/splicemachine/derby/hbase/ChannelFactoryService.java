package com.splicemachine.derby.hbase;

import com.splicemachine.pipeline.client.RpcChannelFactory;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class ChannelFactoryService{
    @SuppressWarnings("unchecked")
    public static RpcChannelFactory loadChannelFactory(){
        ServiceLoader<RpcChannelFactory> serviceLoader = ServiceLoader.load(RpcChannelFactory.class);
        Iterator<RpcChannelFactory> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No ChannelFactory found!");

        return iter.next();
    }

}
