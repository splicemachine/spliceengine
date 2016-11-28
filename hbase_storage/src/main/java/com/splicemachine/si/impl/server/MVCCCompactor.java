/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.si.impl.server;

import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.si.impl.txn.CommittedTxn;
import com.splicemachine.storage.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * An SICompactor
 * @author Scott Fines
 *         Date: 11/23/16
 */
public class MVCCCompactor implements Compactor{
    private static final Logger LOG =Logger.getLogger(MVCCCompactor.class);

    private final long minimumActiveTxn;
    private final TxnSupplier txnCache;
    private final int matApplicationThreshold;

    private long firstTxnBelowMAT = -1L;
    private long matDeleteLine = -1L;
    /*
     * The MAT logic should only apply in the following circumstances:
     *
     * 1. The MAT itself is a number >0 (i.e. 0 disables MAT, since it's the lowest possible transaction id).
     * 2. there are enough cells in the list to justify the application.
     *
     * the second component is a bit tricky to compute. Essentially, we set a threshold, and
     * if the number of versions in the input list exceeds that threshold, we want to use it. In general,
     * we know that there will be a user data or tombstone cell for every version in the list, while
     * there may NOT be a commit timestamp. So we need to know how many true versions there are.
     * We use a rough heuristic, where if the number of entries in the list exceeds twice the threshold,
     * we assume it's enough to justify the MAT.
     *
     */
    private boolean matApplies = false;
    private ByteEntryAccumulator matAccumulator;
    private EntryPredicateFilter matPredFilter;
    private EntryDecoder matFieldDecoder;

    private final SortedSet<Cell> returnData = new TreeSet<>(KeyValue.COMPARATOR);
    private final HCell currentCell = new HCell();

    public MVCCCompactor(TxnSupplier txnStore,
                         long minimumActiveTxn,
                         int matApplicationThreshold,
                         int activeTxnCacheSize){
        this.minimumActiveTxn=minimumActiveTxn;
        this.matApplicationThreshold = 2*matApplicationThreshold;
        this.txnCache = new ActiveTxnCacheSupplier(txnStore,activeTxnCacheSize);
    }

    @Override
    public void compact(List<Cell> input,List<Cell> destination) throws IOException{
        matApplies = minimumActiveTxn>0 && input.size()>matApplicationThreshold;
        returnData.clear();
        if(matPredFilter!=null){
            matAccumulator.reset();
            matPredFilter.reset();
        }
        for(Cell inputCell:input){
            currentCell.set(inputCell);
            mutate(currentCell);
        }

        if(matPredFilter!=null){
            long ts = firstTxnBelowMAT;
            byte[] mergedValue=matAccumulator.finish();
            Cell c = new KeyValue(currentCell.keyArray(),currentCell.keyOffset(),currentCell.keyLength(),
                    SIConstants.DEFAULT_FAMILY_BYTES,0,SIConstants.DEFAULT_FAMILY_BYTES.length,
                    SIConstants.PACKED_COLUMN_BYTES,0,SIConstants.PACKED_COLUMN_BYTES.length,
                    ts,KeyValue.Type.Put,
                    mergedValue,0,mergedValue.length);
            ensureCommitTimestampPresent(c,txnCache.getTransaction(ts));
            returnData.add(c);
        }
        destination.addAll(returnData);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void mutate(HCell currentCell) throws IOException{
        switch(currentCell.dataType()){
            case COMMIT_TIMESTAMP:
                processCommitTimestamp(currentCell);
                break;
            case TOMBSTONE:
                processTombstone(currentCell);
                break;
            case ANTI_TOMBSTONE:
                processAntiTombstone(currentCell);
                break;
            case USER_DATA:
                processUserData(currentCell);
                break;
            default:
                if(LOG.isDebugEnabled()){
                    SpliceLogUtils.debug(LOG,"KeyValue %s is not SI-managed, ignoring",currentCell.stringify());
                }
                returnData.add(currentCell.unwrapDelegate());
        }
    }

    private void processUserData(HCell currentCell) throws IOException{
        long ts = currentCell.version();
        if(ts<matDeleteLine){
            /*
             * we are below the MAT delete line, so we should discard this
             * even if it was committed (it was deleted, and it can't be active).
             */
            return;
        }

        TxnView txn = txnCache.getTransaction(ts);
        switch(txn.getEffectiveState()){
            case ACTIVE:
                /*
                 * By definition, we are above the MAT (since below it implies we are not active),
                 * so we need to keep it, and we don't have to merge it with anything
                 */
                returnData.add(currentCell.unwrapDelegate());
                break;
            case ROLLEDBACK:
                //data was rolled back, so remove the cell
                return;
            case COMMITTED:
                if(matApplies && ts<minimumActiveTxn){
                    if(ts>firstTxnBelowMAT){
                        ensureCommitTimestampPresent(currentCell.unwrapDelegate(),txn);
                        firstTxnBelowMAT=ts;
                        /*
                         * since we are discarding the user data, we also need to discard any
                         * commit timestamp that may exist as well
                         */
                        returnData.removeIf(cell -> cell.getTimestamp()<ts);
                    } else if(ts==firstTxnBelowMAT){
                        /*
                         * since we are discarding the user data, we also need to discard any
                         * commit timestamp that may exist as well
                         */
                        returnData.removeIf(cell -> cell.getTimestamp()<ts);
                    }
                    accumulateData(currentCell);
                }else{
                    ensureCommitTimestampPresent(currentCell.unwrapDelegate(),txn);
                    returnData.add(currentCell.unwrapDelegate());
                }
        }
    }

    private void accumulateData(HCell currentCell) throws IOException{
        byte[] value = currentCell.valueArray();
        int len = currentCell.valueLength();
        int off = currentCell.valueOffset();

        if(matPredFilter==null){
            matPredFilter = EntryPredicateFilter.emptyPredicate();
            matAccumulator = new ByteEntryAccumulator(matPredFilter,true,null);
            matFieldDecoder = new EntryDecoder();
        }
        matFieldDecoder.set(value,off,len);
        Indexed idx =matFieldDecoder.getCurrentIndex();
        matPredFilter.match(idx,matFieldDecoder,matAccumulator);
    }

    private void ensureCommitTimestampPresent(Cell element,TxnView txn){
        byte[] value = Bytes.toBytes(txn.getEffectiveCommitTimestamp());
        returnData.add( new KeyValue(element.getRowArray(),
                element.getRowOffset(),
                element.getRowLength(),
                SIConstants.DEFAULT_FAMILY_BYTES,0,1,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,
                element.getTimestamp(),KeyValue.Type.Put,value,0,value.length));

    }

    private void processAntiTombstone(HCell currentCell) throws IOException{
        long ts = currentCell.version();
        if(ts<matDeleteLine){
            /*
             * We are below the MAT delete line, so we should discard this
             * even if the data is committed (it was deleted, after all). This way,
             * we can save a bit on processing the transaction for an anti-tombstone
             * that we are going to get rid of anyway
             */
            return;
        }
        TxnView txn = txnCache.getTransaction(ts);
        switch(txn.getEffectiveState()){
            case ACTIVE:
                /*
                 * By definition, this cannot occur if the txn is below the MAT,
                 * which means that we need to keep it.
                 */
                returnData.add(currentCell.unwrapDelegate());
                break;
            case ROLLEDBACK:
                //the insert was rolled back, so just remove the cell
                return;
            case COMMITTED:
                if(ts<minimumActiveTxn){
                    /*
                     * If we are committed, and below the MAT, then this anti-tombstone
                     * needs to be merged together with other below-MAT data. However, we know
                     * two facts:
                     *
                     * 1. Anti-tombstones are always accompanied by user data (the insert)
                     * 2. Anti-tombstones are always immediately preceeded (in version) by a tombstone.
                     *
                     * Since we know that we haven't seen a tombstone higher than this timestamp (or
                     * we would have filtered it out ahead of us), we know that the tombstone that we will
                     * see shortly will cause everything below it to be deleted. As a result,
                     * we can discard this Anti-tombstone cell (since there is no delete below us after
                     * we are done cleaning everything up). Hence, we can discard this cell.
                     *
                     * However, it is possible that this was committed with no corresponding commit
                     * timestamp, in which case this timestamp may actually be above the known
                     * "first commit below MAT", so we need to update that field
                     *
                     */
                    if(ts>firstTxnBelowMAT){
                        firstTxnBelowMAT = ts;
                    }
                }else{
                    /*
                     * Anti-tombstone data is always associated with user data, so
                     * we can rely on the associated user data to add our commit timestamp
                     * for us. Hooray saved effort!
                     */
                    returnData.add(currentCell.unwrapDelegate());
                }
        }
    }

    private void processCommitTimestamp(HCell currentCell){
        long ts = currentCell.version();
        if(!txnCache.transactionCached(ts)){
            assert currentCell.valueLength()>0; //shouldn't happen, but let's be safe
            long commitTs = currentCell.valueAsLong();
            txnCache.cache(new CommittedTxn(ts,commitTs));
        }
        if(matApplies && ts<minimumActiveTxn){
            /*
             * Data is received in timestamp order, so the first timestamp
             * that we find below the MAT is our accumulation timestamp.
             */
            if(firstTxnBelowMAT<0){
                firstTxnBelowMAT=ts;
                /*
                 * This is the commit timestamp for our merged MAT cell.
                 */
                returnData.add(currentCell.unwrapDelegate());
            }
        }else
            returnData.add(currentCell.unwrapDelegate()); //keep the cell around
    }

    private void processTombstone(HCell cell) throws IOException{
        long ts = currentCell.version();
        TxnView txn = txnCache.getTransaction(ts);
        switch(txn.getEffectiveState()){
            case ACTIVE:
                /*
                 * By definition, this cannot occur if the transaction is below the MAT,
                 * which means we need to keep it, since it's above the MAT and not rolled back.
                 */
                returnData.add(cell.unwrapDelegate());
                break;
            case ROLLEDBACK:
                /*
                 * The delete transaction has been rolled back, so just remove the cell
                 * from physical storage.
                 */
                return;
            case COMMITTED:
                /*
                 * The tombstone indicates a delete. If the delete occurs above the MAT,
                 * then we keep it as usual. If it occurs below the MAT, then we determine
                 * if this is the "delete line" or not. Specifically, if this is the first
                 * delete to occur below the MAT, then this is the "delete line", and everything
                 * below this should be discarded as deleted. If this is not the first
                 * delete to occur below the MAT, then this should be discarded as a prior delete.
                 *
                 * Note that for deletes we disregard the MAT application threshold (we always delete
                 * below the MAT if the data is deleted).
                 *
                 * Note also that this may result in commit timestamps existing with no corresponding
                 * user data, so we need to clean the commit timestamp cells as well.
                 */
                if(ts<minimumActiveTxn){
                    if(ts>firstTxnBelowMAT){
                        firstTxnBelowMAT = ts;
                    }
                    if(ts<matDeleteLine){
                        //this cell (and everything else) has been deleted. discard it
                        return;
                    }else{
                        matDeleteLine = ts;
                        //clean the commit timestamps which occur for cells below this one
                        returnData.removeIf(c -> c.getTimestamp()<=ts);
                    }
                }else{
                    //this delete occurs above the MAT, so keep hold of it
                    ensureCommitTimestampPresent(currentCell.unwrapDelegate(),txn);
                    returnData.add(cell.unwrapDelegate());
                }
                break;
        }
    }
}

