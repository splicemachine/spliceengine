package com.splicemachine.derby.hbase;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.coprocessor.SpliceMessage.SpliceDerbyCoprocessorService;
import com.splicemachine.coprocessor.SpliceMessage.SpliceSplitServiceRequest;
import com.splicemachine.coprocessor.SpliceMessage.SpliceSplitServiceResponse;
import com.splicemachine.derby.impl.job.coprocessor.BytesCopyTaskSplitter;
import com.splicemachine.derby.impl.stats.Hbase98TableStatsDecoder;
import com.splicemachine.derby.impl.stats.TableStatsDecoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * This class implements both CoprocessorService and RegionServerObserver.  One instance will be created for each
 * region and one instance for the RegionServerObserver interface.  We should probably consider splitting this into
 * two classes.
 */
public class SpliceDerbyCoprocessor extends SpliceDerbyCoprocessorService implements CoprocessorService, Coprocessor {

    private static final Logger LOG = Logger.getLogger(SpliceDerbyCoprocessor.class);

    private HRegion region;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     */
    @Override
    public void start(CoprocessorEnvironment e) {
        TableStatsDecoder.setInstance(new Hbase98TableStatsDecoder());
        SpliceBaseDerbyCoprocessor impl = new SpliceBaseDerbyCoprocessor();
        impl.start(e);
        region = ((RegionCoprocessorEnvironment) e).getRegion();
    }

    /**
     * Logs the stop of the observer and shutdowns the SpliceDriver if needed...
     */
    @Override
    public void stop(CoprocessorEnvironment e) {
    }

    @Override
    public Service getService() {
        return this;
    }

    @Override
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

    @Override
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
        return BytesCopyTaskSplitter.getCutPoints(region, beginKey, endKey);
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
        Store store = region.getStore(SpliceConstants.DEFAULT_FAMILY_BYTES);
        try {
            HRegionUtil.lockStore(store);
            return region.getStore(SpliceConstants.DEFAULT_FAMILY_BYTES).getStoreSizeUncompressed();
        } finally {
                HRegionUtil.unlockStore(store);
        }
    }

}