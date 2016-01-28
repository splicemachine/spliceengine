package com.splicemachine.hbase;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/26/16
 */
public class RegionSizeEndpoint extends SpliceMessage.SpliceDerbyCoprocessorService implements Coprocessor{
    private static final Logger LOG=Logger.getLogger(RegionSizeEndpoint.class);
    private HRegion region;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException{
        region = ((RegionCoprocessorEnvironment) env).getRegion();
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException{

    }

    @Override
    public void computeSplits(RpcController controller,
                              SpliceMessage.SpliceSplitServiceRequest request,
                              RpcCallback<SpliceMessage.SpliceSplitServiceResponse> callback) {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "computeSplits");
        SpliceMessage.SpliceSplitServiceResponse.Builder writeResponse = SpliceMessage.SpliceSplitServiceResponse.newBuilder();
        try {
            ByteString beginKey = request.getBeginKey();
            ByteString endKey = request.getEndKey();
            List<byte[]> splits = computeSplits(region, beginKey.toByteArray(), endKey.toByteArray());
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"computeSplits with beginKey=%s, endKey=%s, numberOfSplits=%s",beginKey,endKey,splits.size());
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

    /* ****************************************************************************************************************/
    /*private helper methods*/
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
