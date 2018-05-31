package com.splicemachine.si.impl.functions;

import com.splicemachine.access.impl.data.UnsafeRecord;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.SpliceQuery;
import com.splicemachine.si.impl.txn.CommittedTxn;
import com.splicemachine.si.impl.txn.RolledBackTxn;
import com.splicemachine.storage.*;
import org.apache.spark.unsafe.Platform;
import org.mortbay.util.SingletonList;

import java.io.IOException;
import java.util.*;

/**
 *
 *
 * Logic for hitting redo log...
 *
 * Not Visible && Not RolledBack && Version > 1
 *
 * Created by jleach on 9/26/17.
 */
public class ScannerFunctionUtils {

    public static long[] getTxnIDs(DataCell[] dataCells, UnsafeRecord unsafeRecord) {
        long[] txnID = new long[dataCells.length];
        for (int i = 0; i< dataCells.length; i++)
            txnID[i] = getTxnID(dataCells[i],unsafeRecord);
        return txnID;
    }

    public static long getTxnID(DataCell dataCell, UnsafeRecord unsafeRecord) {
        if (dataCell == null)
            return -1L;
        unsafeRecord.wrap(dataCell);
        //System.out.println("getTxnID - > " + unsafeRecord);
        return unsafeRecord.getTxnId1();
    }

    public static long[] getEffectiveTimestamps(DataCell[] dataCells, UnsafeRecord unsafeRecord) {
        long[] txnID = new long[dataCells.length];
        for (int i = 0; i< dataCells.length; i++)
            txnID[i] = getEffectiveTimestamp(dataCells[i],unsafeRecord);
        return txnID;
    }

    public static long getEffectiveTimestamp(DataCell dataCell, UnsafeRecord unsafeRecord) {
        if (dataCell == null)
            return -2L;
        unsafeRecord.wrap(dataCell);
        return unsafeRecord.getEffectiveTimestamp();
    }

    public static HashSet<Long> getTxnsToLookup(long[] txnIds, long[] effectiveTimestamps, TxnView currentTxn)  {
        HashSet<Long> longHashSet = new HashSet<Long>();
        for (int i = 0; i< txnIds.length; i++) {
            if (txnIds[i] == -1)
                break;
            if (currentTxn==null || currentTxn.getTxnId()!=txnIds[i]) {
                if (effectiveTimestamps[i] == 0 && !longHashSet.contains(txnIds[i]))
                    longHashSet.add(txnIds[i]);
            }
        }
        return longHashSet;
    }

    public static long getTxnsToLookup(long txnId, long effectiveTimestamp, TxnView currentTxn)  {
        if (txnId == -1)
            return txnId;
        if (currentTxn==null || currentTxn.getTxnId()!=txnId) {
            if (effectiveTimestamp == 0)
                return txnId;
        }
        return -1L;
    }


    public static HashMap<Long,TxnView> getTxns(TxnView currentTxn, TxnSupplier txnSupplier, Set<Long> txnsToLookup) throws IOException {
        return txnSupplier.getTransactions(currentTxn, txnsToLookup);
    }

    public static TxnView getTxn(TxnView currentTxn, TxnSupplier txnSupplier, long txnToLookup) throws IOException {
        return (txnToLookup != -1L)?txnSupplier.getTransaction(currentTxn, txnToLookup):null;
    }

    public static void handleRolledbackAndCommittedTxns(HashMap<Long,TxnView> currentMap, long[] txnIds, long[] effectiveTimestamps) {
        for (int i = 0; i< effectiveTimestamps.length; i++) {
            if (effectiveTimestamps[i] == -1) {
                if (!currentMap.containsKey(txnIds[i])) {
                    currentMap.put(txnIds[i],new RolledBackTxn(txnIds[i]));
                }
            } else if (effectiveTimestamps[i] > 0) {
                if (!currentMap.containsKey(txnIds[i])) {
                    currentMap.put(txnIds[i],new CommittedTxn(txnIds[i],effectiveTimestamps[i]));
                }
            }
        }
    }

    public static TxnView handleRollbackAndCommittedTxn(TxnView lookedUpTxn, long txnId, long effectiveTimestamp) {
        if (effectiveTimestamp == -1) {
            return new RolledBackTxn(txnId);
        } else if (effectiveTimestamp > 0) {
            return new CommittedTxn(txnId,effectiveTimestamp);
        }
        return lookedUpTxn;
    }

    public static BitSet isVisible(HashMap<Long,TxnView> currentMap,long[] txnIds, TxnView currentTxn) {
        BitSet bitSet = new BitSet(txnIds.length);
        for (int i = 0; i< txnIds.length; i++) {
            if (txnIds[i] != -1 && (currentTxn.getTxnId() == txnIds[i] || currentTxn.canSee(currentMap.get(txnIds[i])))) {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    public static void rollForward(HashMap<Long,TxnView> currentMap,long[] txnIds, DataCell[] bufferedRows, Partition partition, TxnView currentTxn) {
        for (int i = 0; i < txnIds.length; i++) {
            TxnView txn = currentMap.get(txnIds[i]);
            if (txnIds[i] != -1 && currentTxn.getTxnId() != txnIds[i] && txn.getEffectiveState().isFinal()) {
                DataCell dc = bufferedRows[i];
                if (Platform.getLong(dc.valueArray(), dc.valueOffset() + UnsafeRecord.EFF_TS_INC) == 0) {
                    partition.fastRollForward(dc, txn.getEffectiveCommitTimestamp());
                }
            }
        }
    }

    public static boolean isVisible(TxnView lookedUpTxn,long txnId, TxnView currentTxn) {
        return (txnId != -1 && (currentTxn.getTxnId() == txnId || currentTxn.canSee(lookedUpTxn)));
    }

    public static DataCell[] getRedoDataCells(DataCell[] dataCells, BitSet isVisible, UnsafeRecord unsafeRecord) {
        DataCell[] redoCells = null;
        for (int i = 0; i < dataCells.length; i++) {
            if (dataCells[i] != null && !isVisible.get(i)) {
                unsafeRecord.wrap(dataCells[i]);
                if (unsafeRecord.getVersion() > 1) {
                    if (redoCells == null)
                        redoCells = new DataCell[dataCells.length];
                    redoCells[i] = dataCells[i];
                }
            }
        }
        return redoCells;
    }

    public static DataCell getRedoDataCells(DataCell dataCell, Boolean isVisible, UnsafeRecord unsafeRecord) {
        if (dataCell != null && !isVisible) {
            unsafeRecord.wrap(dataCell);
            if (unsafeRecord.getVersion() > 1)
                return dataCell;
        }
        return null;
    }

    public static void applyRedoLogic(DataCell[] dataCells, BitSet globalVisibility, DataCell[] redoCells, UnsafeRecord unsafeRecord, Partition p, DataScanner redoScanner, TxnView currentTxn, TxnSupplier txnSupplier, SpliceQuery spliceQuery, OperationFactory operationFactory, TxnOperationFactory txnOperationFactory) throws IOException {
        assert spliceQuery!=null: "SpliceQuery passed in is null";
        try {
            if (redoCells != null) {
                for (int i = 0; i < redoCells.length; i++) {
                    if (redoCells[i] != null) {
                        if (redoScanner == null) {
                            DataScan scan = txnOperationFactory.newDataScan(currentTxn).setFamily(SIConstants.DEFAULT_FAMILY_REDO_BYTES).startKey(redoCells[i].key());//SpliceUtils.createScan(txn, scanColumnList != null && scanColumnList.anySetBit() == -1); // Here is the count(*) piece
                            scan.returnAllVersions();
                            redoScanner = p.openScanner(scan);
                        } else {
                            redoScanner.reseek(redoCells[i].key());
                        }
                        while (true) {
                            List<DataCell> cells = redoScanner.next(1);
                           // System.out.println("Redo cell -> " + cells);
                            DataCell[] actRedoCells = new DataCell[1];
                            unsafeRecord.wrap(dataCells[i]);
                            //System.out.println("Unsafe cell -> " + unsafeRecord);
                            UnsafeRecord redo = new UnsafeRecord();
                            redo.wrap(cells.get(0));
                            //System.out.println("Redo cell -> " + unsafeRecord);
                            if (spliceQuery.getScanColumns() != null) {
                                if (spliceQuery.getTemplate().length() != spliceQuery.getScanColumns().getNumBitsSet()) {
                                    throw new IOException("issue");
                                }
                            }


                            Record update = unsafeRecord.applyRedo(redo, spliceQuery.getTemplate().getVariableLengthBitSet(),spliceQuery.getScanColumns());
                            //System.out.println("Valid Record -> " + updates[0]);
                            actRedoCells[0] =operationFactory.newCell(dataCells[i].key(),SIConstants.DEFAULT_FAMILY_ACTIVE_BYTES,SIConstants.PACKED_COLUMN_BYTES,update.getVersion(),update.getValue());
                            dataCells[i] = actRedoCells[0];
                            long[] txnIds = getTxnIDs(actRedoCells, unsafeRecord);
                            long[] effectiveTxnIds = getEffectiveTimestamps(actRedoCells, unsafeRecord);
                            HashSet<Long> txnsToLookup = getTxnsToLookup(txnIds, effectiveTxnIds, currentTxn);
                            HashMap<Long, TxnView> currentMap = getTxns(currentTxn, txnSupplier, txnsToLookup);
                            handleRolledbackAndCommittedTxns(currentMap, txnIds, effectiveTxnIds);
                            BitSet isVisible = isVisible(currentMap, txnIds, currentTxn);
                            if (isVisible.get(0)) {
                                globalVisibility.set(i);
                                break;
                            }
                            DataCell[] nextRedoCells = getRedoDataCells(actRedoCells, isVisible, unsafeRecord);
                            if (nextRedoCells == null) {
                                break;
                            }
                        }
                    }
                }
            }
        } catch (StandardException se) {
            throw new IOException(se);
        }
    }


    public static boolean applyRedoLogic(DataCell[] dataCell, Boolean globalVisibility, DataCell redoCell, UnsafeRecord unsafeRecord, Partition p, TxnView currentTxn, TxnSupplier txnSupplier, SpliceQuery spliceQuery, OperationFactory operationFactory, TxnOperationFactory txnOperationFactory) throws IOException {
        DataScanner redoScanner = null;
        try {
            if (redoCell != null) {
                DataScan scan = txnOperationFactory.newDataScan(currentTxn).setFamily(SIConstants.DEFAULT_FAMILY_REDO_BYTES).startKey(redoCell.key());//SpliceUtils.createScan(txn, scanColumnList != null && scanColumnList.anySetBit() == -1); // Here is the count(*) piece
                scan.batchCells(1); // one at a time...
                redoScanner = p.openScanner(scan);
                while (true) {
                    List<DataCell> cells = redoScanner.next(1);
                    assert !cells.isEmpty(): "cells cannot be empty";
                    DataCell[] actRedoCells = new DataCell[1];
                    unsafeRecord.wrap(dataCell[0]);
                    UnsafeRecord redo = new UnsafeRecord();
                    redo.wrap(cells.get(0));
                    Record update = unsafeRecord.applyRedo(redo, spliceQuery.getTemplate().getVariableLengthBitSet(),spliceQuery.getScanColumns());
                    actRedoCells[0] =operationFactory.newCell(dataCell[0].key(),SIConstants.DEFAULT_FAMILY_ACTIVE_BYTES,SIConstants.PACKED_COLUMN_BYTES,update.getVersion(),update.getValue());
                    dataCell[0] = actRedoCells[0];
                    long txnId = getTxnID(actRedoCells[0], unsafeRecord);
                    long effectiveTxnId = getEffectiveTimestamp(actRedoCells[0], unsafeRecord);
                    long txnsToLookup = getTxnsToLookup(txnId, effectiveTxnId, currentTxn);
                    TxnView rTxn = getTxn(currentTxn, txnSupplier, txnsToLookup);
                    rTxn = handleRollbackAndCommittedTxn(rTxn, txnId, effectiveTxnId);
                    Boolean isVisible = isVisible(rTxn, txnId, currentTxn);
                    if (isVisible) {
                        globalVisibility = true;
                        return true;
                    }
                    DataCell nextRedoCell = getRedoDataCells(actRedoCells[0], globalVisibility, unsafeRecord);
                    if (nextRedoCell == null) {
                        return false;
                    }
                }
            }
            return false;
        } catch (StandardException se) {
            throw new IOException(se);
        } finally {
            if (redoScanner != null)
                redoScanner.close();
        }
    }

    public static DataResult transformResult(DataResult dataResult, TxnView currentTxn, TxnSupplier txnSupplier, Partition p, SpliceQuery spliceQuery, OperationFactory operationFactory, TxnOperationFactory txnOperationFactory) throws IOException {
        if (dataResult == null) {
            //System.out.println("null");
            return dataResult;
        }
        UnsafeRecord unsafeRecord = new UnsafeRecord();
        DataCell cell = dataResult.activeData();
        long txnId = getTxnID(cell, unsafeRecord);
        long effectiveTxnId = getEffectiveTimestamp(cell, unsafeRecord);
        long txnLookup = getTxnsToLookup(txnId, effectiveTxnId, currentTxn);
        TxnView txn = getTxn(currentTxn, txnSupplier, txnLookup);
        txn = handleRollbackAndCommittedTxn(txn, txnId, effectiveTxnId);
        boolean isVisible = isVisible(txn, txnId, currentTxn);
        DataCell redoCell = getRedoDataCells(cell,isVisible,unsafeRecord);
        DataCell[] cellArray = new DataCell[1];
        cellArray[0] = cell;
        if (redoCell != null)
            isVisible = applyRedoLogic(cellArray,isVisible,redoCell,unsafeRecord,p,currentTxn,txnSupplier,spliceQuery,operationFactory,txnOperationFactory);
        if (isVisible) {
            unsafeRecord.wrap(cellArray[0]);
            //System.out.println("hmm -> (!)" + unsafeRecord);
            if (!unsafeRecord.hasTombstone())
                return operationFactory.newResult(SingletonList.newSingletonList(cellArray[0]));
        }
        //System.out.println("nada (!)");
        return operationFactory.newResult(Collections.emptyList());
    }

    public static DataCell transformCell(DataCell dataResult, TxnView currentTxn, TxnSupplier txnSupplier, Partition p, SpliceQuery spliceQuery, OperationFactory operationFactory, TxnOperationFactory txnOperationFactory) throws IOException {
        if (dataResult == null)
            return dataResult;
        UnsafeRecord unsafeRecord = new UnsafeRecord();
        DataCell cell = dataResult;
        long txnId = getTxnID(cell, unsafeRecord);
        long effectiveTxnId = getEffectiveTimestamp(cell, unsafeRecord);
        long txnLookup = getTxnsToLookup(txnId, effectiveTxnId, currentTxn);
        TxnView txn = getTxn(currentTxn, txnSupplier, txnLookup);
        txn = handleRollbackAndCommittedTxn(txn, txnId, effectiveTxnId);
        boolean isVisible = isVisible(txn, txnId, currentTxn);
        DataCell redoCell = getRedoDataCells(cell,isVisible,unsafeRecord);
        DataCell[] cellArray = new DataCell[1];
        cellArray[0] = cell;
        if (redoCell != null)
            isVisible = applyRedoLogic(cellArray,isVisible,redoCell,unsafeRecord,p,currentTxn,txnSupplier,spliceQuery,operationFactory,txnOperationFactory);
        if (isVisible) {
            unsafeRecord.wrap(cellArray[0]);
            if (!unsafeRecord.hasTombstone())
                return cellArray[0];
        }
        return null;
    }
}
