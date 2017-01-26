/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.hbase;

import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.Region;
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
        try {
            DatabaseLifecycleManager.manager().shutdown();
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG,"splice machine shut down with error",e);
        }
    }

    @Override
    public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext, Region region, Region region2) throws IOException {
        
    }

    @Override
    public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext, Region region, Region region2, Region region3) throws IOException {

    }

    @Override
    public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext, Region region, Region region2, @MetaMutationAnnotation List<Mutation> mutations) throws IOException {

    }

    @Override
    public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext, Region region, Region region2, Region region3) throws IOException {

    }

    @Override
    public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext, Region region, Region region2) throws IOException {

    }

    @Override
    public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext, Region region, Region region2) throws IOException {

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
        return endpoint;
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
