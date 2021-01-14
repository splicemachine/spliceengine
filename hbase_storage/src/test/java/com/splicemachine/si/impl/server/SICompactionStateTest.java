package com.splicemachine.si.impl.server;

import com.splicemachine.concurrent.SameThreadExecutorService;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplier;
import com.splicemachine.si.impl.store.IgnoreTxnSupplier;
import com.splicemachine.si.impl.store.TestingTxnStore;
import com.splicemachine.si.impl.txn.CommittedTxn;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.junit.Test;
import org.mockito.internal.matchers.Same;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SICompactionStateTest {

    public SICompactionStateTest() throws IOException {
    }

    @Test
    public void testIgnoresCommittedTxnsWithoutCommitTimestamp() throws IOException {
        TxnSupplier delegate = mock(TxnSupplier.class);
        when(delegate.getTransaction(0x200L, false)).thenReturn(new CommittedTxn(0x200L, 0x300L));
        when(delegate.getTransaction(0x400L, false)).thenReturn(new CommittedTxn(0x400L, 0x500L));

        IgnoreTxnSupplier ignoreTxnSupplier = new IgnoreTxnSupplier() {
            @Override
            public boolean shouldIgnore(Long txnId) throws IOException { return txnId > 0x400L && txnId < 0x600L; }
            @Override
            public void refresh() {}
        };
        CompletedTxnCacheSupplier txnCacheSupplier = new CompletedTxnCacheSupplier(delegate, 100, 10, ignoreTxnSupplier);
        SICompactionState state = new SICompactionState(txnCacheSupplier, 100,
                new SimpleCompactionContext(), SameThreadExecutorService.instance(), ignoreTxnSupplier);

        List<Cell> data = Arrays.asList(
                SITestUtils.getMockValueCell(0x400L, new boolean[]{true, false, false}),
                SITestUtils.getMockValueCell(0x200L, new boolean[]{true, true, true})
        );

        List<Cell> results = resolveData(state, data);

        assertEquals(2, results.size());
        for (Cell cell : results) {
            if (CellUtil.matchingQualifier(cell, SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES)) {
                assertEquals(0x200L, cell.getTimestamp());
                assertArrayEquals(Bytes.toBytes(0x300L), CellUtil.cloneValue(cell));
            } else if (CellUtil.matchingQualifier(cell, SIConstants.PACKED_COLUMN_BYTES)) {
                assertEquals(0x200L, cell.getTimestamp());
            }
        }
    }

    @Test
    public void testIgnoresCommittedTxnsWithCommitTimestamp() throws IOException {
        TxnSupplier delegate = mock(TxnSupplier.class);
        when(delegate.getTransaction(0x200L, false)).thenReturn(new CommittedTxn(0x200L, 0x300L));
        when(delegate.getTransaction(0x400L, false)).thenReturn(new CommittedTxn(0x400L, 0x500L));

        IgnoreTxnSupplier ignoreTxnSupplier = new IgnoreTxnSupplier() {
            @Override
            public boolean shouldIgnore(Long txnId) throws IOException { return txnId > 0x400L && txnId < 0x600L; }
            @Override
            public void refresh() {}
        };
        CompletedTxnCacheSupplier txnCacheSupplier = new CompletedTxnCacheSupplier(delegate, 100, 10, ignoreTxnSupplier);
        SICompactionState state = new SICompactionState(txnCacheSupplier, 100,
                new SimpleCompactionContext(), SameThreadExecutorService.instance(), ignoreTxnSupplier);

        List<Cell> data = Arrays.asList(
                SITestUtils.getMockCommitCell(0x400L, 0x500L),
                SITestUtils.getMockCommitCell(0x200L, 0x300L),
                SITestUtils.getMockValueCell(0x400L, new boolean[]{true, false, false}),
                SITestUtils.getMockValueCell(0x200L, new boolean[]{true, true, true})
        );

        List<Cell> results = resolveData(state, data);

        // We expect 3 cells: 2 Commit cells and only 1 value cell
        // Currently we don't remove the CommitTimestamp for the ignored txn
        assertEquals(3, results.size());
        for (Cell cell : results) {
            if (CellUtil.matchingQualifier(cell, SIConstants.PACKED_COLUMN_BYTES)) {
                assertEquals(0x200L, cell.getTimestamp());
            }
        }

    }

    private List<Cell> resolveData(SICompactionState state, List<Cell> data) throws IOException {
        PurgeConfig purgeDuringFlush = new PurgeConfigBuilder().purgeDeletesDuringFlush().purgeUpdates(true)
                .transactionLowWatermark(0x800L).build();

        List<Future<TxnView>> futures = state.resolve(data);
        List<Cell> results = new ArrayList<>();
        List<TxnView> txns = futures.stream().map(f -> {
            try {
                return f != null ? f.get() : null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        state.mutate(data, txns, results, purgeDuringFlush);
        return results;
    }
}
