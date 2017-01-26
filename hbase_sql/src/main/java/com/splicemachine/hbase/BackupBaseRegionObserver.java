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

package com.splicemachine.hbase;

import com.google.common.collect.ImmutableList;
import com.splicemachine.coprocessor.SpliceMessage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
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

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

/**
 * Created by jyuan on 2/18/16.
 */
public abstract class BackupBaseRegionObserver extends SpliceMessage.BackupCoprocessorService implements RegionObserver {

    public void start(CoprocessorEnvironment e) throws IOException {
    }

    public void stop(CoprocessorEnvironment e) throws IOException {
    }

    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    }

    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
    }

    public void postLogReplay(ObserverContext<RegionCoprocessorEnvironment> e) {
    }

    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) throws IOException {
    }

    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
    }

    public InternalScanner preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, KeyValueScanner memstoreScanner, InternalScanner s) throws IOException {
        return s;
    }

    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    }

    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    }

    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner) throws IOException {
        return scanner;
    }

    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile) throws IOException {
    }

    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    }

    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c, byte[] splitRow) throws IOException {
    }

    public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] splitKey, List<Mutation> metaEntries) throws IOException {
    }

    public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
    }

    public void preRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
    }

    public void postRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
    }

    public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
    }

    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws IOException {
    }

    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<StoreFile> candidates) throws IOException {
    }

    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<StoreFile> candidates, CompactionRequest request) throws IOException {
        this.preCompactSelection(c, store, candidates);
    }

    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, ImmutableList<StoreFile> selected) {
    }

    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, ImmutableList<StoreFile> selected, CompactionRequest request) {
        this.postCompactSelection(c, store, selected);
    }

    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner, ScanType scanType) throws IOException {
        return scanner;
    }

    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner, ScanType scanType, CompactionRequest request) throws IOException {
        return this.preCompact(e, store, scanner, scanType);
    }

    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s) throws IOException {
        return s;
    }

    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s, CompactionRequest request) throws IOException {
        return this.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s);
    }

    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile) throws IOException {
    }

    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile, CompactionRequest request) throws IOException {
        this.postCompact(e, store, resultFile);
    }

    public void preGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, Result result) throws IOException {
    }

    public void postGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, Result result) throws IOException {
    }

    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
    }

    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
    }

    public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> e, Get get, boolean exists) throws IOException {
        return exists;
    }

    public boolean postExists(ObserverContext<RegionCoprocessorEnvironment> e, Get get, boolean exists) throws IOException {
        return exists;
    }

    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
    }

    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
    }

    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
    }

    public void prePrepareTimeStampForDeleteVersion(ObserverContext<RegionCoprocessorEnvironment> e, Mutation delete, Cell cell, byte[] byteNow, Get get) throws IOException {
    }

    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
    }

    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    }

    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    }

    public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> ctx, MiniBatchOperationInProgress<Mutation> miniBatchOp, boolean success) throws IOException {
    }

    public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, ByteArrayComparable comparator, Put put, boolean result) throws IOException {
        return result;
    }

    public boolean preCheckAndPutAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, ByteArrayComparable comparator, Put put, boolean result) throws IOException {
        return result;
    }

    public boolean postCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, ByteArrayComparable comparator, Put put, boolean result) throws IOException {
        return result;
    }

    public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
        return result;
    }

    public boolean preCheckAndDeleteAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
        return result;
    }

    public boolean postCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, ByteArrayComparable comparator, Delete delete, boolean result) throws IOException {
        return result;
    }

    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append) throws IOException {
        return null;
    }

    public Result preAppendAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> e, Append append) throws IOException {
        return null;
    }

    public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append, Result result) throws IOException {
        return result;
    }

    public long preIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
        return amount;
    }

    public long postIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> e, byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL, long result) throws IOException {
        return result;
    }

    public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> e, Increment increment) throws IOException {
        return null;
    }

    public Result preIncrementAfterRowLock(ObserverContext<RegionCoprocessorEnvironment> e, Increment increment) throws IOException {
        return null;
    }

    public Result postIncrement(ObserverContext<RegionCoprocessorEnvironment> e, Increment increment, Result result) throws IOException {
        return result;
    }

    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        return s;
    }

    public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s) throws IOException {
        return s;
    }

    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        return s;
    }

    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
        return hasMore;
    }

    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
        return hasMore;
    }

    public boolean postScannerFilterRow(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, byte[] currentRow, int offset, short length, boolean hasMore) throws IOException {
        return hasMore;
    }

    public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s) throws IOException {
    }

    public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s) throws IOException {
    }

    public void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> env, HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    }

    public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> env, HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
        this.preWALRestore(env, info, (WALKey)logKey, logEdit);
    }

    public void postWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> env, HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    }

    public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> env, HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
        this.postWALRestore(env, info, (WALKey)logKey, logEdit);
    }

    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths) throws IOException {
    }

    public boolean postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths, boolean hasLoaded) throws IOException {
        return hasLoaded;
    }

    public StoreFile.Reader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf, Reference r, StoreFile.Reader reader) throws IOException {
        return reader;
    }

    public StoreFile.Reader postStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf, Reference r, StoreFile.Reader reader) throws IOException {
        return reader;
    }

    public Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> ctx, MutationType opType, Mutation mutation, Cell oldCell, Cell newCell) throws IOException {
        return newCell;
    }

    public void postStartRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx, HRegion.Operation op) throws IOException {
    }

    public void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx, HRegion.Operation op) throws IOException {
    }

    public DeleteTracker postInstantiateDeleteTracker(ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker) throws IOException {
        return delTracker;
    }
}
