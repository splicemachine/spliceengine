package com.splicemachine.si.impl.region;

import com.splicemachine.si.api.server.ServerControl;
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
        this.regionNameAsString = region.getRegionNameAsString();
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
