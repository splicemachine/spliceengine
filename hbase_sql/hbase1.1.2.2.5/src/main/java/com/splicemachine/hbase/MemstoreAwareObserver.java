package com.splicemachine.hbase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

import java.io.IOException;
import java.util.NavigableSet;

/**
 * Created by jyuan on 4/12/19.
 */
public class MemstoreAwareObserver extends BaseMemstoreAwareObserver{
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                                      Store store,
                                      InternalScanner scanner,
                                      ScanType scanType,
                                      CompactionRequest request) throws IOException {
        return preCompactAction(e, store, scanner, scanType, request);
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile, CompactionRequest request) throws IOException{
        postCompactAction(e, store, resultFile, request);
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c,byte[] splitRow) throws IOException{
        preSplitAction(c, splitRow);
    }

    @Override
    public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException{
        postCompleteSplitAction(e);
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e,Store store,InternalScanner scanner) throws IOException{
        return preFlushAction(e, store, scanner);
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e,Store store,StoreFile resultFile) throws IOException{
        postFlushAction(e, store, resultFile);
    }

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) throws IOException {
        preCloseAction(c, abortRequested);
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
        postCloseAction(e, abortRequested);
    }

    @Override
    public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s) throws IOException{
        return preStoreScannerOpenAction(c, store, scan, targetCols, s);
    }

    @Override
    public void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> env,
                              HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    }

    @Override
    public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> env, HRegionInfo info,
                              HLogKey logKey, WALEdit logEdit) throws IOException {
        preWALRestore(env, info, (WALKey)logKey, logEdit);
    }

    @Override
    public StoreFile.Reader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
                                                   FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
                                                   Reference r, StoreFile.Reader reader) throws IOException {
        return reader;
    }

    @Override
    public StoreFile.Reader postStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
                                                    FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
                                                    Reference r, StoreFile.Reader reader) throws IOException {
        return reader;
    }

    @Override
    public void postWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> env,
                               HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    }

    @Override
    public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> env,
                               HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
        postWALRestore(env, info, (WALKey)logKey, logEdit);
    }

    @Override
    public DeleteTracker postInstantiateDeleteTracker(
            final ObserverContext<RegionCoprocessorEnvironment> ctx, DeleteTracker delTracker)
            throws IOException {
        return delTracker;
    }
}
