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

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;

import java.io.IOException;
import java.util.List;

/**
 * Created by jyuan on 4/11/19.
 */
public abstract class BaseRegionServerObserver implements RegionServerObserver, Coprocessor{
    
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env)
            throws IOException { }

    
    public void start(CoprocessorEnvironment env) throws IOException { }

    
    public void stop(CoprocessorEnvironment env) throws IOException { }

    
    public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx, Region regionA,
                         Region regionB) throws IOException { }

    
    public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c, Region regionA,
                          Region regionB, Region mergedRegion) throws IOException { }

    
    public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                               Region regionA, Region regionB, List<Mutation> metaEntries) throws IOException { }

    
    public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                Region regionA, Region regionB, Region mergedRegion) throws IOException { }

    
    public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                 Region regionA, Region regionB) throws IOException { }

    
    public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                  Region regionA, Region regionB) throws IOException { }

    
    public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
            throws IOException { }

    
    public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
            throws IOException { }

    
    public ReplicationEndpoint postCreateReplicationEndPoint(
            ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
        return endpoint;
    }

    
    public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                       List<AdminProtos.WALEntry> entries, CellScanner cells) throws IOException { }

    
    public void postReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                        List<AdminProtos.WALEntry> entries, CellScanner cells) throws IOException { }
}
