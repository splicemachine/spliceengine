package com.splicemachine.si.impl.functions;

import com.splicemachine.access.impl.data.UnsafeRecord;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.SpliceQuery;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataScanner;
import com.splicemachine.storage.Partition;
import org.mortbay.util.SingletonList;
import scala.tools.nsc.javac.JavaScanners;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;
import static com.splicemachine.si.impl.functions.ScannerFunctionUtils.*;

/**
 *
 *
 */
public class Version3DataScanner implements DataScanner {
    private int batchSize;
    private DataScanner dataScanner;
    DataCell[] bufferedRows;
    UnsafeRecord unsafeRecord;
    private TxnView currentTxn;
    private TxnSupplier txnSupplier;
    private BitSet isVisible;
    private int batchIdx = 0;
    private Partition p;
    private DataScanner redoScanner;
    private OperationFactory operationFactory;
    private TxnOperationFactory txnOperationFactory;
    private SpliceQuery spliceQuery;
    private int count = 0;

    public Version3DataScanner(DataScanner dataScanner, int batchSize,
                               TxnView currentTxn, TxnSupplier txnSupplier, Partition p,
                               SpliceQuery spliceQuery, OperationFactory operationFactory, TxnOperationFactory txnOperationFactory) {
        assert spliceQuery!=null:"Passed in ExecRow is null";
        this.dataScanner = dataScanner;
        this.batchSize = batchSize;
        this.bufferedRows = new DataCell[batchSize];
        this.unsafeRecord = new UnsafeRecord();
        this.currentTxn = currentTxn;
        this.txnSupplier = new ActiveTxnCacheSupplier(txnSupplier);
        this.batchIdx = batchSize;
        this.p = p;
        this.spliceQuery = spliceQuery;
        this.operationFactory = operationFactory;
        this.txnOperationFactory = txnOperationFactory;
    }

    protected List<DataCell> nextDataCells() throws IOException {
        return dataScanner.next(-1);
    }

    @Nonnull
    @Override
    public List<DataCell> next(int limit) throws IOException {
        while (true) {
            if (batchSize < batchIdx+1) {
                for (int i = 0; i < batchSize; i++) {
                    List<DataCell> dataCells = nextDataCells();
                    if (dataCells.isEmpty()) {
                        bufferedRows[i] = null;
                    } else {
                        count++;
                        bufferedRows[i] = dataCells.get(0);
                    }
                }

                long[] txnIds = getTxnIDs(bufferedRows, unsafeRecord);
                long[] effectiveTxnIds = getEffectiveTimestamps(bufferedRows, unsafeRecord);
                HashSet<Long> txnsToLookup = getTxnsToLookup(txnIds, effectiveTxnIds, currentTxn);
                HashMap<Long, TxnView> currentMap = getTxns(currentTxn, txnSupplier, txnsToLookup);
                handleRolledbackAndCommittedTxns(currentMap, txnIds, effectiveTxnIds);
                isVisible = isVisible(currentMap, txnIds, currentTxn);
                rollForward(currentMap,txnIds, bufferedRows, p, currentTxn);
                DataCell[] redoCells = getRedoDataCells(bufferedRows,isVisible,unsafeRecord);
                if (redoCells != null)
                    applyRedoLogic(bufferedRows,isVisible,redoCells,unsafeRecord,p,redoScanner,currentTxn,txnSupplier,spliceQuery,operationFactory,txnOperationFactory);
                batchIdx = 0;
            }
            if (isVisible.get(batchIdx)) {
                unsafeRecord.wrap(bufferedRows[batchIdx]);
                if (unsafeRecord.hasTombstone()) {
                    batchIdx++;
                    continue;
                }
                return SingletonList.newSingletonList(bufferedRows[batchIdx++]);
            } else {
                if (bufferedRows[batchIdx] == null) {
                    batchIdx++;
                    //               System.out.println( "null scan term");
                    return Collections.emptyList();
                }
                batchIdx++; // Skip
                continue;
            }
        }
    }

    @Override
    public TimeView getReadTime() {
        return dataScanner.getReadTime();
    }

    @Override
    public long getBytesOutput() {
        return dataScanner.getBytesOutput();
    }

    @Override
    public long getRowsFiltered() {
        return dataScanner.getRowsFiltered();
    }

    @Override
    public long getRowsVisited() {
        return dataScanner.getRowsVisited();
    }

    @Override
    public void close() throws IOException {
        dataScanner.close();
        if (redoScanner != null)
            redoScanner.close();
    }

    @Override
    public Partition getPartition() {
        return dataScanner.getPartition();
    }

    @Override
    public boolean reseek(byte[] rowKey) throws IOException {
        throw new UnsupportedOperationException("Not Supported Exception");
    }
}
