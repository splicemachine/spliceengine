package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * This class exists just to initiate splice shutdown.  It implements RegionServerObserver and does this in the
 * preStopRegionServer() lifecycle method.
 * <p/>
 * This coprocessor must be listed in hbase.coprocessor.regionserver.classes (region server) which is a different
 * configuration parameter than hbase.coprocessor.region.classes where we list most of our coprocessors.  Only the
 * RegionServerObserver interface appears to have a method that is invoked when the entire server (not just one region
 * on the server) is stopping.  preStopRegionServer() is only invoked if this class is configured in
 * hbase.coprocessor.regionserver.classes.
 *
 * This class used to be combined with SpliceDerbyCoprocessor but that was confusing because that class has per-region
 * concerns but had to implement 15 methods of RegionServerObserver just to invoke shutdown pre region server stop--
 * better to have separate classes I think.
 */
public class ShutdownRegionServerObserver implements RegionServerObserver {

    private static final Logger LOG = Logger.getLogger(ShutdownRegionServerObserver.class);

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env) throws IOException {
        LOG.warn("shutting down splice on this node/JVM");
        SpliceDriver.driver().shutdown();
    }

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

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // RegionServerObserver interface methods added in hbase 1.0
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    //    @Override
    public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
    }

    //    @Override
    public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
    }

    //    @Override
    public ReplicationEndpoint postCreateReplicationEndPoint(ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
        return null;
    }

    //    @Override
    public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx, List<AdminProtos.WALEntry> entries, CellScanner cells) throws IOException {
    }

    //    @Override
    public void postReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx, List<AdminProtos.WALEntry> entries, CellScanner cells) throws IOException {
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // Coprocessor interface
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
    }
}
