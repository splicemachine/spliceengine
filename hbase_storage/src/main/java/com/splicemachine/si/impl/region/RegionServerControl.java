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

package com.splicemachine.si.impl.region;

import com.splicemachine.access.api.ServerControl;
import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/14/15
 */
public class RegionServerControl implements ServerControl{
    private final HRegion region;
    private String regionNameAsString;

    public RegionServerControl(HRegion region){
        this.region=region;
        this.regionNameAsString = region.getRegionInfo().getRegionNameAsString();
    }

    @Override
    public void startOperation() throws IOException{
       region.startRegionOperation();
    }

    @Override
    public void stopOperation() throws IOException{
        region.closeRegionOperation();
    }

    @Override
    public void ensureNetworkOpen() throws IOException{
        checkCallerDisconnect(region,regionNameAsString);
    }

    private static void checkCallerDisconnect(HRegion region, String task) throws CallerDisconnectedException{
        RpcCallContext currentCall = RpcServer.getCurrentCall();
        if(currentCall!=null){
            long afterTime =  currentCall.disconnectSince();
            if(afterTime>0){
                throw new CallerDisconnectedException(
                        "Aborting on region " + region.getRegionInfo().getRegionNameAsString() + ", call " +
                                task + " after " + afterTime + " ms, since " +
                                "caller disconnected");
            }
        }
    }
}
