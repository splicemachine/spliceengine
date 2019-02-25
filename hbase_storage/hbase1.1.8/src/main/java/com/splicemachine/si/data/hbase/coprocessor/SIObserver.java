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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * An HBase coprocessor that applies SI logic to HBase read/write operations.
 */
public class SIObserver extends BaseSIObserver{

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e,Scan scan,RegionScanner s) throws IOException{
        preScannerOpenAction(scan);
        return s;
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,StoreFile resultFile) throws IOException {
        postCompactAction();
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,Delete delete,WALEdit edit,
                          Durability writeToWAL) throws IOException{
        preDeleteAction(e, delete);
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner) throws IOException {
        return preFlushAction(scanner);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,
                                      InternalScanner scanner,ScanType scanType,CompactionRequest compactionRequest) throws IOException{
        return preCompactAction(scanner);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,Put put,WALEdit edit,Durability writeToWAL) throws IOException{
        prePutAction(e, put);
    }


    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append) throws IOException {
        checkAccess();
        return null;
    }

    @Override
    public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
        checkAccess();
        return result;
    }

    @Override
    public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, ByteArrayComparable comparator, Put put, boolean result) throws IOException {
        checkAccess();
        return result;
    }

    @Override
    public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> e, Get get, boolean exists) throws IOException {
        checkAccess();
        return exists;
    }

    @Override
    public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> e, Increment increment) throws IOException {
        checkAccess();
        return null;
    }

    @Override
    public void preGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, Result result) throws IOException {
        checkAccess();
        super.preGetClosestRowBefore(e, row, family, result);
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        checkAccess();
    }

    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths) throws IOException {
        checkAccess();
    }

    public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> env, HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
        this.preWALRestore(env, info, (WALKey) logKey, logEdit);
    }

    public void postWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> env, HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    }

    public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> env, HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
        this.postWALRestore(env, info, (WALKey)logKey, logEdit);
    }

    public StoreFile.Reader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf, Reference r, StoreFile.Reader reader) throws IOException {
        return reader;
    }

    public StoreFile.Reader postStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf, Reference r, StoreFile.Reader reader) throws IOException {
        return reader;
    }

    public DeleteTracker postInstantiateDeleteTracker(ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker) throws IOException {
        return delTracker;
    }
}
