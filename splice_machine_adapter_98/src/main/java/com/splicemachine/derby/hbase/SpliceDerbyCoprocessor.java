package com.splicemachine.derby.hbase;

import com.google.protobuf.Service;
import com.splicemachine.coprocessor.SpliceMessage.SpliceDerbyCoprocessorService;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.List;

public class SpliceDerbyCoprocessor extends SpliceDerbyCoprocessorService implements CoprocessorService, RegionServerObserver {

    private SpliceBaseDerbyCoprocessor impl;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     */
    @Override
    public void start(CoprocessorEnvironment e) {
        impl = new SpliceBaseDerbyCoprocessor();
        impl.start(e);
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
}
