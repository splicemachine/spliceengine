/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.client.ClientRegionConstants;
import com.splicemachine.access.client.MemstoreAware;
import com.splicemachine.access.client.MemstoreKeyValueScanner;
import com.splicemachine.collections.EmptyNavigableSet;
import com.splicemachine.compactions.SpliceCompactionRequest;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.ObserverContextImpl;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Basic concurrency testing on MemstoreAwareObserver
 */
@Ignore
public class MemstoreAwareObserverTest {

    private static final String REGION_NAME = "NetherRegion";
    private static ObserverContext<RegionCoprocessorEnvironment> mockCtx;
    private static Store mockStore;
    private static StoreFile mockStoreFile;
    private static InternalScanner mockScanner;
    private static ScanType userScanType;
    private static CompactionRequest mockCompactionReq;

    @BeforeClass
    public static void setup() throws Exception {
        mockCtx = mock(ObserverContext.class);
        mockStore = mock(Store.class);
        mockStoreFile = mock(StoreFile.class);
        mockScanner = mock(InternalScanner.class);
        userScanType = ScanType.USER_SCAN;
        mockCompactionReq = mock(SpliceCompactionRequest.class);
    }

    //==============================================================================================================
    // tests - compactions
    //==============================================================================================================

    @Test
    public void preCompactTestScanner() throws Exception {
        MemstoreAwareObserver mao = new MemstoreAwareObserver();
        InternalScanner in = new StubInternalScanner();
        InternalScanner out = mao.preCompact(mockCtx, mockStore, in, userScanType, null, mockCompactionReq);
        assertEquals(in, out);
    }

    @Test
    public void preCompactTestRequest() throws Exception {
        MemstoreAwareObserver mao = new MemstoreAwareObserver();
        SpliceCompactionRequest compactionRequest = new StubCompactionRequest();

        mao.preCompact(mockCtx, mockStore, mockScanner, userScanType, null, compactionRequest);
        MemstoreAware first = ((StubCompactionRequest)compactionRequest).guinea.get();

        mao.preCompact(mockCtx, mockStore, mockScanner, userScanType, null, compactionRequest);
        MemstoreAware second = ((StubCompactionRequest)compactionRequest).guinea.get();
        assertEquals(first, second);

        SpliceCompactionRequest newCompactionReq = new StubCompactionRequest();
        mao.preCompact(mockCtx, mockStore, mockScanner, userScanType, null, newCompactionReq);
        MemstoreAware turd = ((StubCompactionRequest)newCompactionReq).guinea.get();
        assertEquals(first, turd);
    }

    @Test
    public void prePostCompactTestRequest() throws Exception {
        MemstoreAwareObserver mao = new MemstoreAwareObserver();
        SpliceCompactionRequest compactionRequest = new StubCompactionRequest();

        mao.preCompact(mockCtx, mockStore, mockScanner, userScanType, null, compactionRequest);
        MemstoreAware pre = ((StubCompactionRequest)compactionRequest).guinea.get();
        // preStorefilesRename() will increment compaction count
        compactionRequest.preStorefilesRename();

        mao.postCompact(mockCtx, mockStore, mockStoreFile, null, compactionRequest);
        // afterExecute() will decrement compaction cost
        compactionRequest.afterExecute();
        MemstoreAware post = ((StubCompactionRequest)compactionRequest).guinea.get();
        assertNotEquals(pre, post);
        assertEquals(pre.currentCompactionCount, post.currentCompactionCount);
    }

    @Test
    public void compactionEarlyFailureWorks() throws Exception {
        SpliceCompactionRequest compactionRequest = new SpliceCompactionRequest(Lists.newArrayList());

        // there's been an exception, preCompact() and preStorefilesRename() are not called

        // afterExecute() won't do anything
        compactionRequest.afterExecute();
    }

    //==============================================================================================================
    // tests - splits
    //==============================================================================================================

//    @Test
//    public void prePostSplitTest() throws Exception {
//        MemstoreAwareObserver mao = new MemstoreAwareObserver();
//
//        mao.preSplit(mockCtx, createByteArray(20));
//        MemstoreAware pre = mao.getMemstoreAware();
//        assertEquals(true, pre.splitMerge);
//
//        mao.postCompleteSplit(mockCtx);
//        MemstoreAware post = mao.getMemstoreAware();
//
//        assertEquals(false, post.splitMerge);
//    }

    //==============================================================================================================
    // tests - flushes
    //==============================================================================================================

    @Test
    public void prePostFlushTest() throws Exception {
        MemstoreAwareObserver mao = new MemstoreAwareObserver();

        InternalScanner in = new StubInternalScanner();
        InternalScanner out = mao.preFlush(mockCtx, mockStore, in, null);
        assertEquals(in, out);

        MemstoreAware pre = mao.getMemstoreAware();
        assertEquals(true, pre.flush);

        mao.postFlush(mockCtx, mockStore, mockStoreFile, null);
        MemstoreAware post = mao.getMemstoreAware();

        assertEquals(false, post.flush);
    }

    //==============================================================================================================
    // tests - open scanner
    //==============================================================================================================

    @Test
    public void preStoreScannerOpen() throws Exception {
        MemstoreAwareObserver mao = new MemstoreAwareObserver();

        // create scan, call preStoreScannerOpen

        // env and scan share same start and end keys (partition hit)
        byte[] startKey = createByteArray(13);
        byte[] endKey = createByteArray(24);

        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
        RegionScanner preScanner = mock(RegionScanner.class);

        RegionScanner postScanner = mao.postScannerOpen(fakeCtx, mockScan(startKey, endKey), preScanner);

        assertNotNull(postScanner);
        assertNotEquals(preScanner, postScanner);
        postScanner.close();
    }

    @Test
    public void preStoreScannerOpenPartitionMiss() throws Exception {
        MemstoreAwareObserver mao = new MemstoreAwareObserver();

        // env and scan do not share same start and end keys (partition miss)
        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(createByteArray(13), createByteArray(24));
        RegionScanner preScanner = mock(RegionScanner.class);

        try {
            mao.postScannerOpen(fakeCtx, mockScan(createByteArray(14), createByteArray(25)), preScanner);
            fail("Expected DoNotRetryIOException");
        } catch (IOException e) {
            // expected
            assertTrue(e instanceof DoNotRetryIOException);
        }
    }

    @Test
    public void preStoreScannerOpenWhileCompacting() throws Exception {
        MemstoreAwareObserver mao = new MemstoreAwareObserver();

        // request compaction, open scan, verify exception, close compaction, re-issue scan, verify success

        // env and scan share same start and end keys (partition hit)
        byte[] startKey = createByteArray(13);
        byte[] endKey = createByteArray(24);

        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
        RegionScanner scanner = mock(RegionScanner.class);
        Scan internalScanner = mockScan(startKey, endKey);

        // compacting...
        SpliceCompactionRequest compactionRequest = new StubCompactionRequest();
        mao.preCompact(mockCtx, mockStore, (InternalScanner) internalScanner, userScanType, null, compactionRequest);
        compactionRequest.preStorefilesRename();
        try {
            mao.postScannerOpen(fakeCtx, internalScanner, scanner);
            fail("Expected IOException - compacting");
        } catch (IOException e) {
            // expected
            assertEquals(e.getLocalizedMessage(),
                         String.format("splitting, merging, or active compaction on scan on %s", REGION_NAME));
        }

        // signal compaction complete, open scan again
        compactionRequest.afterExecute();
        mao.postCompact(mockCtx, mockStore, mockStoreFile, null, compactionRequest);

        RegionScanner theScan = mao.postScannerOpen(fakeCtx, internalScanner, scanner);
        assertNotNull(theScan);
        theScan.close();
    }

    @Test
    public void preStoreScannerOpenWhileFlushing() throws Exception {
        MemstoreAwareObserver mao = new MemstoreAwareObserver();

        // request flush, open scan, verify exception, close flush, re-issue scan, verify success

        // env and scan share same start and end keys (partition hit)
        byte[] startKey = createByteArray(13);
        byte[] endKey = createByteArray(24);

        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
        RegionScanner scanner = mock(RegionScanner.class);
        Scan internalScanner = mockScan(startKey, endKey);

        // flushing...
        mao.preFlush(fakeCtx, mockStore, (InternalScanner) scanner, null);
        try {
            mao.postScannerOpen(fakeCtx, internalScanner, scanner);
            fail("Expected IOException - flushing");
        } catch (IOException e) {
            // expected
            assertEquals(e.getLocalizedMessage(),
                         String.format("splitting, merging, or active compaction on scan on %s", REGION_NAME));
        }

        // signal flush complete, open scan again
        mao.postFlush(fakeCtx, mockStore, mockStoreFile, null);

        RegionScanner theScan = mao.postScannerOpen(fakeCtx, internalScanner, scanner);
        assertNotNull(theScan);
        theScan.close();
    }

//    @Test
//    public void preStoreScannerOpenWhileSplitting() throws Exception {
//        MemstoreAwareObserver mao = new MemstoreAwareObserver();
//
//        // request split, open scan, verify exception, close split, re-issue scan, verify success
//
//        // env and scan share same start and end keys (partition hit)
//        byte[] startKey = createByteArray(13);
//        byte[] endKey = createByteArray(24);
//
//        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
//        KeyValueScanner scanner = mock(MemstoreKeyValueScanner.class);
//        Scan internalScanner = mockScan(startKey, endKey);
//        KeyValueScanner theScan;
//
//        // splitting...
//        mao.preSplit(fakeCtx, createByteArray(20));
//        try {
//            mao.preStoreScannerOpen(fakeCtx, mockStore(5000L),
//                                    internalScanner,
//                                    EmptyNavigableSet.<byte[]>instance(), scanner);
//            fail("Expected IOException - splitting");
//        } catch (IOException e) {
//            // expected
//            assertEquals(e.getLocalizedMessage(),
//                         String.format("splitting, merging, or active compaction on scan on %s", REGION_NAME));
//        }
//
//        // signal split complete, open scan again
//        mao.postCompleteSplit(fakeCtx);
//
//        theScan = mao.preStoreScannerOpen(fakeCtx, mockStore(5000L),
//                                            internalScanner,
//                                            EmptyNavigableSet.<byte[]>instance(), scanner);
//        assertNotNull(theScan);
//        theScan.close();
//    }

    //==============================================================================================================
    // threaded tests -- compactions, flushes, splits -- while scanning
    //==============================================================================================================

    @Test
    public void threadedScanThenCompact() throws Exception {
        // create scan, issue, wait
        // create compaction, request, wait
        // verify compaction blocked for duration of scan
        // verify both finish
        // T1: openScanner                            -> closeScanner
        // T2:             -> compactionPause -> wait                 -> compactionPause proceed -> Success


        final MemstoreAwareObserver mao = new MemstoreAwareObserver();
        // env and scan share same start and end keys (partition hit)
        byte[] startKey = createByteArray(13);
        byte[] endKey = createByteArray(24);

        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
        Scan internalScanner = mockScan(startKey, endKey);

        StateOrdering ordering = new StateOrdering();
        final ScanRequestThread scanThread = new ScanRequestThread("ScanReq", mao, fakeCtx,
                                                                   internalScanner, false, ordering);
        final CompactionRequestThread compactionThread = new CompactionRequestThread("CompactionReq", mao,
                                                                                     new StubCompactionRequest(),
                                                                                     (InternalScanner) internalScanner,
                                                                                     false, ordering);
        ordering.registerStart(scanThread.getName());
        ordering.registerStart(compactionThread.getName());
        ordering.registerFinish(scanThread.getName());
        ordering.registerFinish(compactionThread.getName());

        List<Throwable> exceptions = assertConcurrent("threadedScanThenCompact()", Arrays.asList(scanThread,
                                                                                                compactionThread), 60);
        assertTrue(exceptions.toString(), scanThread.completed);
        assertTrue(exceptions.toString(), compactionThread.completed);
        assertTrue("failed with exception(s)" + exceptions, exceptions.isEmpty());
    }

    @Test
    public void threadedCompactThenScan() throws Exception {
        // create compaction, wait
        // create scan, fail
        // after compaction completes, issue scan again, succeed
        // verify compaction and final scan finish
        // T1: (0)compactionPause                         -> (2)Complete
        // T2:                 -> (1)openScanner -> Error             -> (3)openScanner -> Success


        final MemstoreAwareObserver mao = new MemstoreAwareObserver();
        // env and scan share same start and end keys (partition hit)
        byte[] startKey = createByteArray(13);
        byte[] endKey = createByteArray(24);

        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
        Scan internalScanner = mockScan(startKey, endKey);
        StateOrdering ordering = new StateOrdering();
        final ScanRequestThread scanFailThread = new ScanRequestThread("ScanReq1", mao, fakeCtx, internalScanner,

                                                                       false, ordering);
        final CompactionRequestThread compactionThread = new CompactionRequestThread("CompactionReq", mao,
                                                                                     new StubCompactionRequest(),
                                                                                     (InternalScanner) internalScanner,
                                                                                     false, ordering);

        final ScanRequestThread scanSucceedThread = new ScanRequestThread("ScanReq2", mao, fakeCtx,
                                                                          internalScanner,
                                                                          false, ordering);
        ordering.registerStart(compactionThread.getName());
        ordering.registerStart(scanFailThread.getName());
        ordering.registerFinish(compactionThread.getName());
        ordering.registerStart(scanSucceedThread.getName());
        ordering.registerFinish(scanSucceedThread.getName());

        List<Throwable> exceptions = assertConcurrent("threadedCompactThenScan()", Arrays.asList(scanFailThread,
                                                                                                compactionThread,
                                                                                                scanSucceedThread), 20);
        assertFalse(exceptions.toString(), scanFailThread.completed);
        assertTrue(exceptions.toString(), compactionThread.completed);
        assertTrue(exceptions.toString(), scanSucceedThread.completed);
        assertEquals(1, exceptions.size());
        assertEquals("ScanReq1 Failed to start scan.", exceptions.get(0).getLocalizedMessage());
    }

    @Test
    public void threadedFlushThenScan() throws Exception {
        // create flush, wait
        // create scan, fail
        // after flush completes, issue scan again, succeed
        // verify flush and final scan finish
        // T1: flushPause                         -> Complete
        // T2:            -> openScanner -> Error             -> openScanner -> Success

        final MemstoreAwareObserver mao = new MemstoreAwareObserver();
        // env and scan share same start and end keys (partition hit)
        byte[] startKey = createByteArray(13);
        byte[] endKey = createByteArray(24);

        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
        Scan internalScanner = mockScan(startKey, endKey);
        StateOrdering ordering = new StateOrdering();
        final ScanRequestThread scanFailThread = new ScanRequestThread("ScanReq1", mao, fakeCtx, internalScanner,
                                                                       false, ordering);
        final FlushRequestThread flushThread = new FlushRequestThread("FlushReq", mao, fakeCtx,
                                                                      (InternalScanner) internalScanner,
                                                                      false, ordering);

        final ScanRequestThread scanSucceedThread = new ScanRequestThread("ScanReq2", mao, fakeCtx, internalScanner,
                                                                          false, ordering);
        ordering.registerStart(flushThread.getName());
        ordering.registerStart(scanFailThread.getName());
        ordering.registerFinish(flushThread.getName());
        ordering.registerStart(scanSucceedThread.getName());
        ordering.registerFinish(scanFailThread.getName());

        List<Throwable> exceptions = assertConcurrent("threadedFlushThenScan()", Arrays.asList(scanFailThread,
                                                                                                flushThread,
                                                                                                scanSucceedThread), 20);
        assertFalse(exceptions.toString(), scanFailThread.completed);
        assertTrue(exceptions.toString(), flushThread.completed);
        assertTrue(exceptions.toString(), scanSucceedThread.completed);
        assertEquals(exceptions.toString(), 1, exceptions.size());
        assertEquals("ScanReq1 Failed to start scan.", exceptions.get(0).getLocalizedMessage());
    }

//    @Test
//    public void threadedSplitThenScan() throws Exception {
//        // create split, wait
//        // create scan, fail
//        // after split completes, issue scan again, succeed
//        // verify split and final scan finish
//        // T1: splitPause                          -> Complete
//        // T2:            -> openScanner -> Error              -> openScanner -> Success
//
//        final MemstoreAwareObserver mao = new MemstoreAwareObserver();
//        // env and scan share same start and end keys (partition hit)
//        byte[] startKey = createByteArray(13);
//        byte[] endKey = createByteArray(24);
//
//        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
//        Scan internalScanner = mockScan(startKey, endKey);
//        StateOrdering ordering = new StateOrdering();
//        final ScanRequestThread scanFailThread = new ScanRequestThread("ScanReq1", mao, fakeCtx, internalScanner,
//                                                                       false, ordering);
//        final SplitRequestThread splitThread = new SplitRequestThread("SplitReq", mao, fakeCtx,
//                                                                      false, ordering);
//
//        final ScanRequestThread scanSucceedThread = new ScanRequestThread("ScanReq2", mao, fakeCtx, internalScanner,
//                                                                          false, ordering);
//        ordering.registerStart(splitThread.getName());
//        ordering.registerStart(scanFailThread.getName());
//        ordering.registerFinish(splitThread.getName());
//        ordering.registerStart(scanSucceedThread.getName());
//        ordering.registerFinish(scanSucceedThread.getName());
//
//        List<Throwable> exceptions = assertConcurrent("threadedSplitThenScan()", Arrays.asList(scanFailThread,
//                                                                                                splitThread,
//                                                                                                scanSucceedThread), 20);
//        assertFalse(exceptions.toString(), scanFailThread.completed);
//        assertTrue(exceptions.toString(), splitThread.completed);
//        assertTrue(exceptions.toString(), scanSucceedThread.completed);
//        assertEquals(exceptions.toString(), 1, exceptions.size());
//        assertEquals("ScanReq1 Failed to start scan.", exceptions.get(0).getLocalizedMessage());
//    }

    @Test
    public void threadedCompactThenScanThenFlushThenScan() throws Exception {
        // create compaction, wait
        // create scan, fail
        // create flush, wait
        // create scan, fail
        // after compaction completes, issue scan again, fail - flush still active
        // after flush completes, issue scan again, success
        // verify compaction, flush and final scan finish
        // T1: compactionPause                                                               -> Complete
        // T2:                 -> openScanner -> Error               -> openScanner -> Error             -> openScanner -> Error            -> openScanner ->Success
        // T3:                                        -> flushPause                                                             -> Complete


        final MemstoreAwareObserver mao = new MemstoreAwareObserver();
        // env and scan share same start and end keys (partition hit)
        byte[] startKey = createByteArray(13);
        byte[] endKey = createByteArray(24);

        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
        Scan internalScanner = mockScan(startKey, endKey);
        StateOrdering ordering = new StateOrdering();
        final CompactionRequestThread compactionThread = new CompactionRequestThread("CompactionReq", mao,
                                                                                     new StubCompactionRequest(),
                                                                                     (InternalScanner) internalScanner,
                                                                                     false, ordering);
        final ScanRequestThread scanFailThread1 = new ScanRequestThread("ScanReq1", mao, fakeCtx, internalScanner,
                                                                        false, ordering);
        final FlushRequestThread flushThread = new FlushRequestThread("FlushReq", mao, fakeCtx,
                                                                      (InternalScanner) internalScanner,
                                                                      false, ordering);
        final ScanRequestThread scanFailThread2 = new ScanRequestThread("ScanReq2", mao, fakeCtx, internalScanner,
                                                                        false, ordering);
        final ScanRequestThread scanFailThread3 = new ScanRequestThread("ScanReq3", mao, fakeCtx, internalScanner,
                                                                        false, ordering);
        final ScanRequestThread scanSucceedThread = new ScanRequestThread("ScanReq4", mao, fakeCtx, internalScanner,
                                                                          false, ordering);

        ordering.registerStart(compactionThread.getName());
        ordering.registerStart(scanFailThread1.getName());
        ordering.registerStart(flushThread.getName());
        ordering.registerStart(scanFailThread2.getName());
        ordering.registerFinish(compactionThread.getName());
        ordering.registerStart(scanFailThread3.getName());
        ordering.registerFinish(flushThread.getName());
        ordering.registerStart(scanSucceedThread.getName());
        ordering.registerFinish(scanSucceedThread.getName());

        List<Throwable> exceptions = assertConcurrent("threadedCompactThenScanThenFlushThenScan()",
                                                      Arrays.asList(compactionThread,
                                                                    scanFailThread1,
                                                                    flushThread,
                                                                    scanFailThread2,
                                                                    scanFailThread3,
                                                                    scanSucceedThread),
                                                      20);
        assertTrue(exceptions.toString(), compactionThread.completed);
        assertFalse(exceptions.toString(), scanFailThread1.completed);
        assertTrue(exceptions.toString(), flushThread.completed);
        assertFalse(exceptions.toString(), scanFailThread2.completed);
        assertFalse(exceptions.toString(), scanFailThread3.completed);
        assertTrue(exceptions.toString(), scanSucceedThread.completed);
        assertEquals(exceptions.toString(), 3, exceptions.size());
        assertEquals("ScanReq1 Failed to start scan.", exceptions.get(0).getLocalizedMessage());
        assertEquals("ScanReq2 Failed to start scan.", exceptions.get(1).getLocalizedMessage());
        assertEquals("ScanReq3 Failed to start scan.", exceptions.get(2).getLocalizedMessage());
    }

    @Test
    public void threadedCompactThenScanCompactionFails() throws Exception {
        // create compaction, wait
        // create scan, fail
        // compaction fails, issue scan again, succeed
        // verify compaction failed and final scan finish
        // T1: compactionPause                          -> Failure
        // T2:                 -> openScanner -> Error             -> openScanner -> Success

        final MemstoreAwareObserver mao = new MemstoreAwareObserver();
        // env and scan share same start and end keys (partition hit)
        byte[] startKey = createByteArray(13);
        byte[] endKey = createByteArray(24);

        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
        Scan internalScanner = mockScan(startKey, endKey);
        StateOrdering ordering = new StateOrdering();
        final ScanRequestThread scanFailThread = new ScanRequestThread("ScanReq1", mao, fakeCtx, internalScanner,
                                                                       false, ordering);
        final CompactionRequestThread compactionThread = new CompactionRequestThread("CompactionReq", mao,
                                                                                     new StubCompactionRequest(),
                                                                                     (InternalScanner) internalScanner,
                                                                                     true, ordering);

        final ScanRequestThread scanSucceedThread = new ScanRequestThread("ScanReq2", mao, fakeCtx, internalScanner,
                                                                          false, ordering);

        ordering.registerStart(compactionThread.getName());
        ordering.registerStart(scanFailThread.getName());
        ordering.registerFinish(compactionThread.getName());
        ordering.registerStart(scanSucceedThread.getName());
        ordering.registerFinish(scanSucceedThread.getName());

        List<Throwable> exceptions = assertConcurrent("threadedCompactThenScanCompactionFails()",
                                                      Arrays.asList(scanFailThread,compactionThread, scanSucceedThread),
                                                      20);
        assertFalse(exceptions.toString(), scanFailThread.completed);
        assertFalse(exceptions.toString(), compactionThread.completed);
        assertTrue(exceptions.toString(), scanSucceedThread.completed);
        assertEquals(exceptions.toString(), 2, exceptions.size());
        assertEquals("ScanReq1 Failed to start scan.", exceptions.get(0).getLocalizedMessage());
        assertEquals("CompactionReq Induced failure.", exceptions.get(1).getLocalizedMessage());
    }

    @Test
    public void threadedFlushThenScanFlushFails() throws Exception {
        // create flush, wait
        // create scan, fail
        // flush fails, issue scan again, succeed
        // verify flush failed and final scan finish
        // T1: flushPause                           -> Failure
        // T2:            -> openScanner -> Error              -> openScanner -> Success

        final MemstoreAwareObserver mao = new MemstoreAwareObserver();
        // env and scan share same start and end keys (partition hit)
        byte[] startKey = createByteArray(13);
        byte[] endKey = createByteArray(24);

        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
        Scan internalScanner = mockScan(startKey, endKey);
        StateOrdering ordering = new StateOrdering();
        final ScanRequestThread scanFailThread = new ScanRequestThread("ScanReq1", mao, fakeCtx, internalScanner,
                                                                       false, ordering);
        final FlushRequestThread flushThread = new FlushRequestThread("FlushReq", mao,
                                                                      fakeCtx,
                                                                             (InternalScanner) internalScanner,
                                                                      true, ordering);

        final ScanRequestThread scanSucceedThread = new ScanRequestThread("ScanReq2", mao, fakeCtx, internalScanner,
                                                                          false, ordering);
        ordering.registerStart(flushThread.getName());
        ordering.registerStart(scanFailThread.getName());
        ordering.registerFinish(flushThread.getName());
        ordering.registerStart(scanSucceedThread.getName());
        ordering.registerFinish(scanSucceedThread.getName());

        List<Throwable> exceptions = assertConcurrent("threadedFlushThenScanFlushFails()",
                                                      Arrays.asList(scanFailThread,flushThread, scanSucceedThread),
                                                      20);
        assertFalse(exceptions.toString(), scanFailThread.completed);
        assertFalse(exceptions.toString(), flushThread.completed);
        assertTrue(exceptions.toString(), scanSucceedThread.completed);
        assertEquals(exceptions.toString(), 2, exceptions.size());
        assertEquals("ScanReq1 Failed to start scan.", exceptions.get(0).getLocalizedMessage());
        assertEquals("FlushReq Induced failure.", exceptions.get(1).getLocalizedMessage());
    }

//    @Test
//    public void threadedSplitThenScanSplitFails() throws Exception {
//        // create split, wait
//        // create scan, fail
//        // split fails, issue scan again, succeed
//        // verify split failed and final scan finish
//        // T1: splitPause                           -> Failure
//        // T2:            -> openScanner -> Error              -> openScanner -> Success
//
//        final MemstoreAwareObserver mao = new MemstoreAwareObserver();
//        // env and scan share same start and end keys (partition hit)
//        byte[] startKey = createByteArray(13);
//        byte[] endKey = createByteArray(24);
//
//        ObserverContext<RegionCoprocessorEnvironment> fakeCtx = mockRegionEnv(startKey, endKey);
//        Scan internalScanner = mockScan(startKey, endKey);
//        StateOrdering ordering = new StateOrdering();
//        final ScanRequestThread scanFailThread = new ScanRequestThread("ScanReq1", mao, fakeCtx, internalScanner,
//                                                                       false, ordering);
//        final SplitRequestThread splitThread = new SplitRequestThread("SplitReq", mao,
//                                                                      fakeCtx,
//                                                                      true, ordering);
//
//        final ScanRequestThread scanSucceedThread = new ScanRequestThread("ScanReq2", mao, fakeCtx, internalScanner,
//                                                                          false, ordering);
//
//        ordering.registerStart(splitThread.getName());
//        ordering.registerStart(scanFailThread.getName());
//        ordering.registerFinish(splitThread.getName());
//        ordering.registerStart(scanSucceedThread.getName());
//        ordering.registerFinish(scanSucceedThread.getName());
//
//        List<Throwable> exceptions = assertConcurrent("threadedSplitThenScanSplitFails()",
//                                                      Arrays.asList(scanFailThread,splitThread, scanSucceedThread),
//                                                      20);
//        assertFalse(exceptions.toString(), scanFailThread.completed);
//        assertFalse(exceptions.toString(), splitThread.completed);
//        assertTrue(exceptions.toString(), scanSucceedThread.completed);
//        assertEquals(exceptions.toString(), 2, exceptions.size());
//        assertEquals("ScanReq1 Failed to start scan.", exceptions.get(0).getLocalizedMessage());
//        assertEquals("SplitReq Induced failure.", exceptions.get(1).getLocalizedMessage());
//    }

    //==============================================================================================================
    // helpers
    //==============================================================================================================

    private static ObserverContext<RegionCoprocessorEnvironment> mockRegionEnv(byte[] startKey, byte[] endKey) {
        HRegionInfo mockRInfo = mock(HRegionInfo.class);
        when(mockRInfo.getRegionNameAsString()).thenReturn(REGION_NAME);
        when(mockRInfo.getStartKey()).thenReturn(startKey);
        when(mockRInfo.getEndKey()).thenReturn(endKey);

        /*
        ConsistencyControl mockCC = mock(ConsistencyControl.class);
        when(mockCC.memstoreReadPoint()).thenReturn(1313L);
        */
        HRegion mockRegion = mock(HRegion.class);
        when(mockRegion.getRegionInfo()).thenReturn(mockRInfo);

        RegionCoprocessorEnvironment mockEnv = mock(RegionCoprocessorEnvironment.class);
        when(mockEnv.getRegion()).thenReturn(mockRegion);
        when(mockEnv.getRegionInfo()).thenReturn(mockRInfo);
        ObserverContextImpl<RegionCoprocessorEnvironment> fakeCtx = new ObserverContextImpl<RegionCoprocessorEnvironment>(null);
        fakeCtx.prepare(mockEnv);
        return fakeCtx;
    }

    private static Scan mockScan(byte[] startKey, byte[] endKey) {
        Scan scan = new StubInternalScanner();
        scan.setAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY, SIConstants.TRUE_BYTES);
        scan.setAttribute(ClientRegionConstants.SPLICE_SCAN_MEMSTORE_PARTITION_BEGIN_KEY, startKey);
        scan.setAttribute(ClientRegionConstants.SPLICE_SCAN_MEMSTORE_PARTITION_END_KEY, endKey);
        return scan;
    }

    private static Store mockStore(long ttlInMillis) {
        ScanInfo mockInfo = mock(ScanInfo.class);
        when(mockInfo.getTtl()).thenReturn(ttlInMillis);

        HStore mockStore = mock(HStore.class);
        when(mockStore.getScanInfo()).thenReturn(mockInfo);
        return mockStore;
    }

    private static byte[] createByteArray(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static List<Throwable> assertConcurrent(final String message, final List<? extends Callable<Boolean>> runnables,
                                                    final int maxTimeoutSec) throws InterruptedException {
        final int numThreads = runnables.size();
        final List<Throwable> exceptions = new ArrayList<>(runnables.size());
        final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        final CompletionService<Boolean> completionService =new ExecutorCompletionService<>(threadPool);
        try {
            final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
            final CountDownLatch afterInitBlocker = new CountDownLatch(1);
            for (final Callable<Boolean> submittedTestRunnable : runnables) {
                completionService.submit(new Callable<Boolean>(){
                    @Override
                    public Boolean call() throws Exception{
                        allExecutorThreadsReady.countDown();
                        afterInitBlocker.await();
                        return submittedTestRunnable.call();
                    }
                });
            }
            // wait until all threads are ready
            assertTrue("Timeout initializing threads! Perform long lasting initializations before starting test runners",
                       allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS));
            // start all test runners
            afterInitBlocker.countDown();

            long timeRemainingMillis = TimeUnit.SECONDS.toMillis(maxTimeoutSec);
            int remaining = runnables.size();
            while(timeRemainingMillis > 0 && remaining > 0){
                long s = System.currentTimeMillis();
                try{
                    Future<Boolean> poll = completionService.poll(remaining,TimeUnit.MILLISECONDS);
                    if(poll!=null){
                        if (poll.isDone() || poll.isCancelled()) {
                            remaining--;
                            poll.get(); //check for errors
                        }
                    }
                }catch(ExecutionException ee){
                    // DEBUG:
//                    System.out.println("TopE: "+ ee.getLocalizedMessage());
                    exceptions.add(ee.getCause());
                } finally{
                    timeRemainingMillis-=(System.currentTimeMillis()-s);
                }
            }
            assertTrue(message+" timeout! more than "+ maxTimeoutSec+" seconds",timeRemainingMillis>0);
        } finally {
            threadPool.shutdownNow();
        }
        return exceptions;
    }

    //==============================================================================================================
    // Nested classes
    //==============================================================================================================

    private abstract class ThreadTest implements Callable<Boolean> {
        protected final StateOrdering ordering;
        protected final RegionObserver mao;
        protected final boolean shouldFail;
        boolean completed = false;
        private final String name;

        protected ThreadTest(StateOrdering ordering, String name,
                             RegionObserver mao,
                             boolean shouldfail) {
            this.ordering = ordering;
            this.name = name;
            this.mao = mao;
            this.shouldFail = shouldfail;
        }

        @Override
        public Boolean call() throws Exception {
            // DEBUG:
//            System.out.println("\n"+getName()+" Call.");
            waitForStart();
            try {
                runMe();
                completed = true;
            } finally {
                triggerComplete();
            }
            // DEBUG
//            System.out.println(getName()+" Completed.");
            return completed;
        }

        protected void triggerComplete(){
            System.out.println(getName()+" Finishing.");
            ordering.advance(getName());
        }

        protected void waitForStart() throws InterruptedException{
            ordering.awaitStage(name+ StateOrdering.START_STAGE);
        }

        protected void waitForFinish() throws InterruptedException{
            ordering.advanceAndAwaitStage(name+ StateOrdering.FINISH_STAGE);
        }

        protected String getName(){
            return name;
        }

        @Override
        public String toString(){
            return name;
        }

        public abstract void runMe() throws Exception;
    }

    private class ScanRequestThread extends ThreadTest {
        private final ObserverContext<RegionCoprocessorEnvironment> fakeCtx;
        private final Scan internalScanner;

        private ScanRequestThread(String name,
                                  MemstoreAwareObserver mao,
                                  ObserverContext<RegionCoprocessorEnvironment> fakeCtx,
                                  Scan internalScanner, boolean shouldFail, StateOrdering ordering) {
            super(ordering, name, mao, shouldFail);
            this.fakeCtx = fakeCtx;
            this.internalScanner = internalScanner;
        }

        @Override
        public void runMe() throws Exception {
            // DEBUG
            System.out.println(getName()+" Starting scan.");
            RegionScanner theScan;
            try {
                theScan = mao.postScannerOpen(fakeCtx, internalScanner,mock(RegionScanner.class));
            } catch (IOException e) {
                throw new RuntimeException(getName()+" Failed to start scan.",e);
            }
            System.out.println(getName()+" Started scan.");
            waitForFinish();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                System.out.println(getName()+" Scan interrupted.");
            }
            try {
                if (shouldFail) {
                    throw new RuntimeException(getName()+" Induced failure.");
                }
            } finally {
                // TODO: JC - that scan is _always_ closed, regardless of success or failure, is ASSUMED
                try {
                    // DEBUG:
//                    System.out.println(getName()+" Closing Scan.");
                    theScan.close();
                } catch (Exception e) {
                    throw new RuntimeException(getName()+" Failed to close scan.",e);
                }
            }
            System.out.println(getName()+" Ended scan.");
        }
    }

    private class CompactionRequestThread extends ThreadTest {
        private final SpliceCompactionRequest compactionRequest;
        private final InternalScanner internalScanner;

        private CompactionRequestThread(String name, MemstoreAwareObserver mao,
                                        SpliceCompactionRequest compactionRequest,
                                        InternalScanner internalScanner, boolean shouldFail, StateOrdering ordering) {
            super(ordering, name, mao, shouldFail);
            this.compactionRequest = compactionRequest;
            this.internalScanner = internalScanner;
        }

        @Override
        public void runMe() throws Exception {
            //compactionRequest.beforeExecute();
            try{
                // DEBUG:
                System.out.println(getName()+" Starting compaction.");
                try {
                    mao.preCompact(mockCtx,mockStore,internalScanner,userScanType,null,compactionRequest);
                    // DEBUG:
//                    System.out.println(getName()+" preStorefilesRename begin.");
                    // next compactionRequest call will block. advance stage so other threads can work.
                    ordering.advance(getName());
                    compactionRequest.preStorefilesRename();
                    // DEBUG:
//                    System.out.println(getName()+" preStorefilesRename end.");
                } catch (IOException e) {
                    throw new RuntimeException(getName()+" Failed to start compaction.",e);
                }
                // DEBUG:
//                System.out.println(getName()+" Compaction started.");
                // wait for Compaction.Complete stage
                ordering.awaitStage(getName()+ StateOrdering.FINISH_STAGE);
                if(shouldFail){
                    throw new RuntimeException(getName()+" Induced failure.");
                }
                // signal compaction complete, open scan again
                try {
                    mao.postCompact(mockCtx,mockStore,mockStoreFile,null,compactionRequest);
                } catch (IOException e) {
                    throw new RuntimeException(getName()+" Failed to terminate compaction.",e);
                }
            }finally{
                compactionRequest.afterExecute();
            }
            // DEBUG:
//            System.out.println(getName()+" Ended compaction.");
        }
    }

    private class FlushRequestThread extends ThreadTest {
        private final ObserverContext<RegionCoprocessorEnvironment> fakeCtx;
        private final InternalScanner internalScanner;

        private FlushRequestThread(String name, MemstoreAwareObserver mao, ObserverContext<RegionCoprocessorEnvironment> fakeCtx,
                                   InternalScanner internalScanner, boolean shouldFail, StateOrdering ordering) {
            super(ordering, name, mao, shouldFail);
            this.fakeCtx = fakeCtx;
            this.internalScanner = internalScanner;
        }

        /**
         * {@link RegionObserver#postFlush(ObserverContext, Store, StoreFile, FlushLifeCycleTracker)}
         * @throws Exception
         */
        @Override
        public void runMe() throws Exception{
            try {
                mao.preFlush(fakeCtx, mockStore, internalScanner,null);
            } catch (IOException e) {
                throw new RuntimeException(getName()+" Failed to start flush.",e);
            }
            waitForFinish();
            try {
                if (shouldFail) {
                    throw new RuntimeException(getName()+" Induced failure.");
                }
            } finally {
                // TODO: JC - that postFlush is _always_ called, regardless of flush success or failure, is ASSUMED
                try {
                    // DEBUG:
//                    System.out.println(getName()+" postFlush.");
                    mao.postFlush(fakeCtx, mockStore, mockStoreFile,null);
                } catch (IOException e) {
                    throw new RuntimeException(getName()+" Failed to terminate flush.",e);
                }
            }
        }
    }

//    private class SplitRequestThread extends ThreadTest {
//        private final ObserverContext<RegionCoprocessorEnvironment> fakeCtx;
//
//        private SplitRequestThread(String name, RegionObserver mao, ObserverContext<RegionCoprocessorEnvironment> fakeCtx,
//                                   boolean shouldFail, StateOrdering ordering) {
//            super(ordering, name, mao, shouldFail);
//            this.fakeCtx = fakeCtx;
//        }
//
//        /**
//         * {@link RegionObserver#postCompleteSplit(ObserverContext)}
//         * {@link RegionObserver#postRollBackSplit(ObserverContext)}
//         * @throws Exception
//         */
//        @Override
//        public void runMe() throws Exception{
//            try {
//                mao.preSplit(fakeCtx, createByteArray(20));
//            } catch (IOException e) {
//                throw new RuntimeException(getName()+" Failed to start split.",e);
//            }
//            waitForFinish();
//            try {
//                if (shouldFail) {
//                    /*
//                     * When a Split fails, then the coprocessor call of "postRollBackSplit" is called,
//                     * but only on RegionObserver instances.
//                     */
//                    mao.postRollBackSplit(fakeCtx);
//                    throw new RuntimeException(getName()+" Induced failure.");
//                }
//            } finally {
//                // HBase calls regardless of success/failure
//                try {
//                    mao.postCompleteSplit(fakeCtx);
//                } catch (IOException e) {
//                    throw new RuntimeException(getName()+" Failed to terminate split.",e);
//                }
//            }
//        }
//    }

    private class StateOrdering{
        public static final String START_STAGE = ".START";
        public static final String FINISH_STAGE = ".FINISH";
        private final Map<String,Integer> nameStageMap = new ConcurrentHashMap<>();
        private final ConcurrentNavigableMap<Integer,CountDownLatch> stageCounterMap = new ConcurrentSkipListMap<>();
        private final AtomicInteger currentStage = new AtomicInteger(0);
        private int stageOrder = 0;

        public void registerStart(String name) {
            registerStage(name+START_STAGE, stageOrder++);
        }

        public void registerFinish(String name) {
            registerStage(name+FINISH_STAGE, stageOrder++);
        }

        public void advance(String name){
            int nextStage = currentStage.incrementAndGet();
            // DEBUG:
//            System.out.println(name+" Advancing. NextStage: "+nextStage);

            Map.Entry<Integer, CountDownLatch> entry=stageCounterMap.ceilingEntry(nextStage);
            if(entry==null) {
                // DEBUG:
//                System.out.println(name+" Done!");
                return; //we are done!
            }
            currentStage.set(entry.getKey());
            entry.getValue().countDown();
        }

        public void advanceAndAwaitStage(String name) throws InterruptedException {
            advance(name);
            awaitStage(name);
        }

        public void awaitStage(String name) throws InterruptedException {
            Integer waitStage = nameStageMap.get(name);
            int cs = currentStage.get();
            // DEBUG
//            System.out.println(name+" Awaiting - CurrentStage: "+cs+" WaitStage: "+waitStage);
            if(waitStage == null || cs >= waitStage)
                return; //return immediately
            else if(waitStage < cs){
                throw new IllegalStateException("Advanced past stage you are waiting on!");
            }
            // DEBUG
//            System.out.println(name+" Awaiting - CurrentStage: "+cs+" == WaitStage: "+waitStage);
            try {
                stageCounterMap.get(waitStage).await(); //wait for my stage to be triggered
            } catch (InterruptedException e) {
                // DEBUG
//                System.out.println(name+" interrupted!");
                throw e;
            }
        }

        private void registerStage(String name, int position){
            CountDownLatch countDownLatch=stageCounterMap.get(position);
            if(countDownLatch!=null){
                throw new IllegalStateException("Stage "+ position+" already registered");
            }else{
                stageCounterMap.put(position,new CountDownLatch(1));
            }
            // DEBUG:
//            System.out.println("Registering "+name+" at stage: "+position);
            nameStageMap.put(name,position);
        }
    }

   private static class StubCompactionRequest extends SpliceCompactionRequest {
       AtomicReference<MemstoreAware> guinea;

       StubCompactionRequest () {
           super(Lists.newArrayList());
       }

       @Override
       public void setMemstoreAware(AtomicReference<MemstoreAware> memstoreAware) {
           super.setMemstoreAware(memstoreAware);
           guinea = memstoreAware;
       }
   }
}

