/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.hbase;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HBasePlatformUtils;
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
public class RegionSizeEndpoint extends SpliceMessage.SpliceDerbyCoprocessorService implements CoprocessorService,Coprocessor{
    private static final Logger LOG=Logger.getLogger(RegionSizeEndpoint.class);
    private HRegion region;
    private String hostName;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException{
        hostName = ((RegionCoprocessorEnvironment) env).getRegionServerServices().getServerName().getHostname();
        region = (HRegion)((RegionCoprocessorEnvironment) env).getRegion();
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException{

    }

    @Override
    public Service getService(){
        return this;
    }

    @Override
    public void computeSplits(RpcController controller,
                              SpliceMessage.SpliceSplitServiceRequest request,
                              RpcCallback<SpliceMessage.SpliceSplitServiceResponse> callback) {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "computeSplits");
        SpliceMessage.SpliceSplitServiceResponse.Builder writeResponse = SpliceMessage.SpliceSplitServiceResponse.newBuilder();
        writeResponse.setHostName(hostName);
        try {
            ByteString beginKey = request.getBeginKey();
            ByteString endKey = request.getEndKey();
            int requestedSplits = 0;
            long bytesPerSplit = 0;
            if (request.hasRequestedSplits()) {
                requestedSplits = request.getRequestedSplits();
            }
            if (request.hasBytesPerSplit()) {
                bytesPerSplit = request.getBytesPerSplit();
            }
            List<byte[]> splits = computeSplits(region, beginKey.toByteArray(), endKey.toByteArray(), requestedSplits, bytesPerSplit);

            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"computeSplits with beginKey=%s, endKey=%s, numberOfSplits=%s, bytesPerSplit=%ld",beginKey,endKey,splits.size(), bytesPerSplit);
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
            writeResponse.setEncodedName(region.getRegionInfo().getRegionNameAsString());
            writeResponse.setSizeInBytes(HBasePlatformUtils.getMemstoreSize(region)+getStoreFileSize());
        } catch (Exception e) {
            org.apache.hadoop.hbase.protobuf.ResponseConverter.setControllerException(controller, new IOException(e));
        }
        callback.run(writeResponse.build());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static List<byte[]> computeSplits(HRegion region, byte[] beginKey, byte[] endKey, int requestedSplits, long bytesPerSplit) throws IOException {
        return BytesCopyTaskSplitter.getCutPoints(region, beginKey, endKey, requestedSplits, bytesPerSplit);
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
