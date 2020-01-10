/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.impl.region;

import com.splicemachine.access.api.ServerControl;
import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.HBasePlatformUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

import java.io.IOException;
import java.util.Optional;

/**
 * @author Scott Fines
 *         Date: 12/14/15
 */
public class RegionServerControl implements ServerControl{
    private final RegionServerServices rsServices;
    private final HRegion region;
    private String regionNameAsString;

    public RegionServerControl(HRegion region,RegionServerServices rsServices){
        this.region=region;
        this.rsServices = rsServices;
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

    @Override
    public boolean isAvailable(){
        return region.isAvailable();
    }

    private static void checkCallerDisconnect(HRegion region, String task) throws CallerDisconnectedException{
        RpcCallContext currentCall = getRpcCallContext();
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

    private static RpcCallContext getRpcCallContext() {
        Optional<RpcCall> rpcCall = RpcServer.getCurrentCall();
        return rpcCall.isPresent() ? rpcCall.get() : null;
    }
}
