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

package com.splicemachine.si.data.hbase.coprocessor;


import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.google.protobuf.Service;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.TableAuthManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Lists;

/**
 * An HBase coprocessor that applies SI logic to HBase read/write operations.
 */
public class SIObserver extends BaseSIObserver implements RegionCoprocessor{
    private static Logger LOG = Logger.getLogger(SIObserver.class);

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan) throws IOException {
        preScannerOpenAction(scan);
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile, CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        postCompactAction();
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
        return preFlushAction(scanner);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        return preCompactAction(scanner);
    }


    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        prePutAction(c, put);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
        preDeleteAction(c, delete);
    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
        checkAccess();
        return super.preAppend(c, append);
    }

    @Override
    public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
                                     byte[] qualifier, CompareOperator op, ByteArrayComparable comparator,
                                     Delete delete, boolean result) throws IOException {
        checkAccess();
        return super.preCheckAndDelete(c, row, family, qualifier, op, comparator, delete, result);
    }

    @Override
    public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, byte[] family,
                                  byte[] qualifier, CompareOperator op, ByteArrayComparable comparator, Put put,
                                  boolean result) throws IOException {
        checkAccess();
        return super.preCheckAndPut(c, row, family, qualifier, op, comparator, put, result);
    }

    @Override
    public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> e, Get get, boolean exists) throws IOException {
        checkAccess();
        return super.preExists(e, get, exists);
    }

    @Override
    public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> e, Increment increment) throws IOException {
        checkAccess();
        return super.preIncrement(e, increment);
    }

    @Override
    public void preGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, Result result) throws IOException {
        checkAccess();
        super.preGetClosestRowBefore(e, row, family, result);
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        checkAccess();
        super.preBatchMutate(c, miniBatchOp);
    }

    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths) throws IOException {
        checkAccess();
        super.preBulkLoadHFile(ctx, familyPaths);
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }
}
