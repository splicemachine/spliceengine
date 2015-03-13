package com.splicemachine.derby.hbase;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.coprocessor.SpliceMessage.SpliceDerbyCoprocessorService;
import com.splicemachine.coprocessor.SpliceMessage.SpliceSplitServiceRequest;
import com.splicemachine.coprocessor.SpliceMessage.SpliceSplitServiceResponse;
import com.splicemachine.derby.impl.job.coprocessor.BytesCopyTaskSplitter;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.List;

public class SpliceDerbyCoprocessor extends SpliceDerbyCoprocessorService implements CoprocessorService, RegionServerObserver {

    private SpliceBaseDerbyCoprocessor impl;
    protected HRegion region;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     */
    @Override
    public void start(CoprocessorEnvironment e) {
        impl = new SpliceBaseDerbyCoprocessor();
        impl.start(e);
        region = ((RegionCoprocessorEnvironment) e).getRegion();
    }

    /**
     * Logs the stop of the observer and shutdowns the SpliceDriver if needed...
     */
    @Override
    public void stop(CoprocessorEnvironment e) {
        impl.stop(e);
    }

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env) throws IOException {
        impl.stoppingRegionServer();
    }

    /************************
     *    Unused
     ***********************/
    @Override
    public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext, HRegion hRegion, HRegion hRegion2) throws IOException {
    }

    @Override
    public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext, HRegion hRegion, HRegion hRegion2, HRegion hRegion3) throws IOException {
    }

    @Override
    public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx, HRegion regionA, HRegion regionB, @MetaMutationAnnotation List<Mutation> metaEntries) throws IOException {

    }

    @Override
    public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext, HRegion hRegion, HRegion hRegion2, HRegion hRegion3) throws IOException {
     }

    @Override
    public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext, HRegion hRegion, HRegion hRegion2) throws IOException {
     }

    @Override
    public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext, HRegion hRegion, HRegion hRegion2) throws IOException {
     }

	@Override
	public void computeSplits(RpcController controller,
			SpliceSplitServiceRequest request,
			RpcCallback<SpliceSplitServiceResponse> callback) {
		SpliceMessage.SpliceSplitServiceResponse.Builder writeResponse = SpliceMessage.SpliceSplitServiceResponse.newBuilder();
		try {
				ByteString beginKey = request.getBeginKey();
				ByteString endKey = request.getEndKey();
				List<byte[]> splits = computeSplits(region, beginKey.toByteArray(),endKey.toByteArray());
				for (byte[] split: splits) 
					writeResponse.addCutPoint(com.google.protobuf.ByteString.copyFrom(split));
		} catch (java.io.IOException e) {
				org.apache.hadoop.hbase.protobuf.ResponseConverter.setControllerException(controller, e);
		}
		callback.run(writeResponse.build());		
	}
	
	public List<byte[]> computeSplits(HRegion region, byte[] beginKey, byte[] endKey) throws IOException {
		return BytesCopyTaskSplitter.getCutPoints(region, beginKey, endKey);
	}
	
}
