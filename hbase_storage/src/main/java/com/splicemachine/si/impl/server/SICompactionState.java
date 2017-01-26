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

package com.splicemachine.si.impl.server;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.si.impl.txn.CommittedTxn;
import com.splicemachine.si.impl.txn.RolledBackTxn;
import com.splicemachine.storage.CellType;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Captures the SI logic to perform when a data table is compacted (without explicit HBase dependencies). Provides the
 * guts for SICompactionScanner.
 * <p/>
 * It is handed key-values and can change them.
 */
public class SICompactionState {
    private static final Logger LOG = Logger.getLogger(SICompactionState.class);
    private final TxnSupplier transactionStore;
    private SortedSet<Cell> dataToReturn;
    private final RollForward rollForward;
    private ByteSlice rowSlice = new ByteSlice();

    public SICompactionState(TxnSupplier transactionStore,RollForward rollForward,int activeTransactionCacheSize) {
        this.rollForward = rollForward;
        this.transactionStore = new ActiveTxnCacheSupplier(transactionStore,activeTransactionCacheSize);
        this.dataToReturn  =new TreeSet<>(KeyValue.COMPARATOR);
    }

    /**
     * Given a list of key-values, populate the results list with possibly mutated values.
     *
     * @param rawList - the input of key values to process
     * @param results - the output key values
     */
    public void mutate(List<Cell> rawList, List<Cell> results) throws IOException {
        dataToReturn.clear();
        for (Cell aRawList : rawList) {
            mutate(aRawList);
        }
        results.addAll(dataToReturn);
    }

    /**
     * Apply SI mutation logic to an individual key-value. Return the "new" key-value.
     */
    private void mutate(Cell element) throws IOException {
        final CellType cellType= getKeyValueType(element);
        long timestamp = element.getTimestamp();
        switch (cellType) {
            case COMMIT_TIMESTAMP:
                /*
                 * Older versions of SI code would put an "SI Fail" element in the commit timestamp
                 * field when a row has been rolled back. While newer versions will just outright delete the entry,
                 * we still need to deal with entries which are in the old form. As time goes on, this should
                 * be less and less frequent, but you still have to check
                 */
                ensureTransactionCached(timestamp,element);
                dataToReturn.add(element);
                return;
            case TOMBSTONE:
            case ANTI_TOMBSTONE:
            case USER_DATA:
                if(mutateCommitTimestamp(timestamp,element))
                    dataToReturn.add(element);
                return;
            default:
                if(LOG.isDebugEnabled()){
                    String famString = Bytes.toString(element.getFamilyArray(),element.getFamilyOffset(),element.getFamilyLength());
                    String qualString = Bytes.toString(element.getQualifierArray(),element.getQualifierOffset(),element.getQualifierLength());
                       SpliceLogUtils.debug(LOG,"KeyValue with family %s and column %s are not SI-managed, ignoring",
                               famString,qualString);
                }
                dataToReturn.add(element);
        }
    }

    private void ensureTransactionCached(long timestamp,Cell element) {
        if(!transactionStore.transactionCached(timestamp)){
            if(isFailedCommitTimestamp(element)){
                transactionStore.cache(new RolledBackTxn(timestamp));
            }else if (element.getValueLength()>0){ //shouldn't happen, but you never know
                long commitTs = Bytes.toLong(element.getValueArray(),element.getValueOffset(),element.getValueLength());
                transactionStore.cache(new CommittedTxn(timestamp,commitTs));
            }
        }
    }

    /**
     * Replace unknown commit timestamps with actual commit times.
     */
    private boolean mutateCommitTimestamp(long timestamp,Cell element) throws IOException {
        TxnView transaction = transactionStore.getTransaction(timestamp);
        if(transaction.getEffectiveState()== Txn.State.ROLLEDBACK){
            /*
             * This transaction has been rolled back, so just remove the data
             * from physical storage
             */
            recordResolved(element,transaction);
            return false;
        }
        TxnView t = transaction;
        while(t.getState()== Txn.State.COMMITTED){
            t = t.getParentTxnView();
        }
        if(t==Txn.ROOT_TRANSACTION){
            /*
             * This element has been committed all the way to the user level, so a
             * commit timestamp can be placed on it.
             */
            long globalCommitTimestamp = transaction.getEffectiveCommitTimestamp();
            dataToReturn.add(newTransactionTimeStampKeyValue(element, Bytes.toBytes(globalCommitTimestamp)));
            recordResolved(element, transaction);
        }
        return true;
    }

    private void recordResolved(Cell element, TxnView transaction) {
        rowSlice.set(element.getRowArray(),element.getRowOffset(),element.getRowLength());
        rollForward.recordResolved(rowSlice,transaction.getTxnId());
    }

    public Cell newTransactionTimeStampKeyValue(Cell element, byte[] value) {
        return new KeyValue(element.getRowArray(),
                element.getRowOffset(),
                element.getRowLength(),
                SIConstants.DEFAULT_FAMILY_BYTES,0,1,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,
                element.getTimestamp(),KeyValue.Type.Put,
                value,0,value==null?0:value.length);
    }


    public boolean isFailedCommitTimestamp(Cell element) {
        return element.getValueLength()==1 && element.getValueArray()[element.getValueOffset()]==SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP[0];
    }

    public CellType getKeyValueType(Cell keyValue) {
        if (CellUtils.singleMatchingQualifier(keyValue,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES)) {
            return CellType.COMMIT_TIMESTAMP;
        } else if (CellUtils.singleMatchingQualifier(keyValue, SIConstants.PACKED_COLUMN_BYTES)) {
            return CellType.USER_DATA;
        } else if (CellUtils.singleMatchingQualifier(keyValue,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES)) {
            if (CellUtils.matchingValue(keyValue, SIConstants.EMPTY_BYTE_ARRAY)) {
                return CellType.TOMBSTONE;
            } else if (CellUtils.matchingValue(keyValue,SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES)) {
                return CellType.ANTI_TOMBSTONE;
            }
        } else if (CellUtils.singleMatchingQualifier(keyValue, SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)) {
            return CellType.FOREIGN_KEY_COUNTER;
        }
        return CellType.OTHER;
    }

}
