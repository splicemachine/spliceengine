package com.splicemachine.hbase;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.splicemachine.SQLConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.coprocessor.SpliceMessage.SpliceSplitServiceRequest;
import com.splicemachine.coprocessor.SpliceMessage.SpliceSplitServiceResponse;
import com.splicemachine.derby.lifecycle.MonitoredLifecycleService;
import com.splicemachine.derby.lifecycle.NetworkLifecycleService;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.RegionServerLifecycle;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.List;

/**
 * This class implements both CoprocessorService and RegionServerObserver.  One instance will be created for each
 * region and one instance for the RegionServerObserver interface.  We should probably consider splitting this into
 * two classes.
 */
public class SpliceDerbyCoprocessor extends BaseRegionServerObserver {
    private static final Logger LOG = Logger.getLogger(SpliceDerbyCoprocessor.class);
    public static volatile String regionServerZNode;
    public static volatile String rsZnode;

    private HRegion region;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     */
    @Override
    public void start(CoprocessorEnvironment e) throws IOException{
        region = ((RegionCoprocessorEnvironment) e).getRegion();
        startEngine(e);
    }

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env) throws IOException{
        DatabaseLifecycleManager.manager().shutdown();
    }

    private void startEngine(CoprocessorEnvironment e) throws IOException{
        RegionServerServices regionServerServices = ((RegionCoprocessorEnvironment) e).getRegionServerServices();

        rsZnode = regionServerServices.getZooKeeper().rsZNode;
        regionServerZNode = regionServerServices.getServerName().getServerName();


        //ensure that the SI environment is booted properly
        HBaseSIEnvironment env=HBaseSIEnvironment.loadEnvironment(new SystemClock(),ZkUtils.getRecoverableZooKeeper());
        SIDriver driver = env.getSIDriver();

        //make sure the configuration is correct
        SConfiguration config=driver.getConfiguration();
        config.addDefaults(SQLConfiguration.defaults);

        DatabaseLifecycleManager manager=DatabaseLifecycleManager.manager();
        //register the engine boot service
        try{
            HBaseConnectionFactory connFactory = HBaseConnectionFactory.getInstance(driver.getConfiguration());
            RegionServerLifecycle distributedStartupSequence=new RegionServerLifecycle(driver.getClock(),connFactory);
            manager.registerEngineService(new MonitoredLifecycleService(distributedStartupSequence,config));

            //register the network boot service
            manager.registerNetworkService(new NetworkLifecycleService(config));
        }catch(Exception e1){
            throw new DoNotRetryIOException(e1);
        }
    }

    /**
     * Logs the stop of the observer and shutdowns the SpliceDriver if needed...
     */
    @Override
    public void stop(CoprocessorEnvironment e) {
    }

//    @Override
    public void computeSplits(RpcController controller,
                              SpliceSplitServiceRequest request,
                              RpcCallback<SpliceSplitServiceResponse> callback) {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "computeSplits");
        SpliceMessage.SpliceSplitServiceResponse.Builder writeResponse = SpliceMessage.SpliceSplitServiceResponse.newBuilder();
        try {
            ByteString beginKey = request.getBeginKey();
            ByteString endKey = request.getEndKey();
            List<byte[]> splits = computeSplits(region, beginKey.toByteArray(), endKey.toByteArray());
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "computeSplits with beginKey=%s, endKey=%s, numberOfSplits=%s", beginKey, endKey, splits.size());
            for (byte[] split : splits)
                writeResponse.addCutPoint(com.google.protobuf.ByteString.copyFrom(split));
        } catch (java.io.IOException e) {
            org.apache.hadoop.hbase.protobuf.ResponseConverter.setControllerException(controller, e);
        }
        callback.run(writeResponse.build());
    }

//    @Override
    public void computeRegionSize(RpcController controller, SpliceMessage.SpliceRegionSizeRequest request, RpcCallback<SpliceMessage.SpliceRegionSizeResponse> callback) {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "computeRegionSize");
        SpliceMessage.SpliceRegionSizeResponse.Builder writeResponse = SpliceMessage.SpliceRegionSizeResponse.newBuilder();
        try {
            writeResponse.setEncodedName(region.getRegionNameAsString());
            writeResponse.setSizeInBytes(region.getMemstoreSize().longValue()+getStoreFileSize());
        } catch (Exception e) {
            org.apache.hadoop.hbase.protobuf.ResponseConverter.setControllerException(controller, new IOException(e));
        }
        callback.run(writeResponse.build());
    }

    private static List<byte[]> computeSplits(HRegion region, byte[] beginKey, byte[] endKey) throws IOException {
        throw new RuntimeException("Not Implemented");
//        return null;//BytesCopyTaskSplitter.getCutPoints(region, beginKey, endKey);
    }

    /**
     * Compute Store File Size.  Performs it under a lock in case store files are changing underneath us.
     *
     * @see HRegionUtil#lockStore(org.apache.hadoop.hbase.regionserver.Store)
     * @see HRegionUtil#unlockStore(org.apache.hadoop.hbase.regionserver.Store)
     *
     * @return
     */
    private long getStoreFileSize() {
        Store store = region.getStore(SIConstants.DEFAULT_FAMILY_BYTES);
        try {
            HRegionUtil.lockStore(store);
            return region.getStore(SIConstants.DEFAULT_FAMILY_BYTES).getStoreSizeUncompressed();
        } finally {
                HRegionUtil.unlockStore(store);
        }
    }

}