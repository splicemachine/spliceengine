package com.splicemachine.si.impl.region;

import com.splicemachine.si.api.txn.TxnTimeTravelResult;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;

public class TxnTimeTraveler {
    private static final Logger LOG=Logger.getLogger(TxnTimeTraveler.class);

    private TxnFinder txnFinder;

    public TxnTimeTraveler(TxnFinder txnFinder) {
        this.txnFinder = txnFinder;
    }

    /// @return Pair<closest tx id, closest tx's ts>
    private Pair<Long, Long> bsearch(long needleTS, byte bucket, byte[] begin, long beginTS,
                                     byte[] end, long endTS) throws IOException {
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"enter bsearch in for range: [%s (TS: %s), %s (TS: %s)]",
                    TxnUtils.rowKeytoHbaseEscaped(begin), new Timestamp(beginTS).toString(),
                    TxnUtils.rowKeytoHbaseEscaped(end), new Timestamp(endTS).toString());
        byte[] left = begin, right = end, middle;
        long leftTS = beginTS, rightTS = endTS;
        boolean changed = true, reversed = false;
        while(changed) {
            if(Arrays.equals(left, right)) {
                if(leftTS < needleTS) {
                    return new Pair<>(txOf(left), leftTS);
                } else {
                    return null;
                }
            }
            changed = false;
            middle = middle(left, right);
            Pair<Long, Long> middleTx;
            middleTx = txnFinder.find(bucket, middle, reversed);
            reversed = false;
            if(LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"middle of range: [%s (TS: %s) id %s, %s (TS: %s)] id %s is %s (TS: %s) id %s",
                        TxnUtils.rowKeytoHbaseEscaped(left), new Timestamp(leftTS).toString(), txOf(left),
                        TxnUtils.rowKeytoHbaseEscaped(right), new Timestamp(rightTS).toString(), txOf(right),
                        TxnUtils.rowKeytoHbaseEscaped(middle), new Timestamp(middleTx.getSecond()).toString(), middleTx.getFirst().toString());
            long middleTS = middleTx.getSecond();
            if(middleTS == needleTS) {
                return middleTx;
            } else if(needleTS < middleTS) {
                if(middleTS == rightTS)
                {
                    reversed = true;
                    changed = true;
                }
                else if(middleTS < rightTS)
                {
                    right = toRowKeyWithBucket(middleTx.getFirst(), bucket);
                    rightTS = middleTS;
                    changed = true;
                }
            } else if(leftTS < middleTS) {
                left = toRowKeyWithBucket(middleTx.getFirst(), bucket);
                leftTS = middleTS;
                changed = true;
            }
        }
        return new Pair<>(txOf(left), leftTS);
    }

    private long txOf(byte[] rowKey) {
        return TxnUtils.txnIdFromRowKey(rowKey, 0, rowKey.length);
    }

    private byte[] middle(byte[] left, byte[] right) {
        assert(left.length == 9); assert(right.length == 9);
        byte lBucket = left[0], rBucket = right[0];
        assert(lBucket == rBucket);
        long lTxnId = txOf(left), rTxnId = txOf(right), middleTxnId = (lTxnId + rTxnId) / 2;
        return toRowKeyWithBucket(middleTxnId, lBucket);
    }

    private Pair<Long, Long> first(byte bucket) throws IOException {
        return txnFinder.find(bucket, null,false);
    }

    private Pair<Long, Long> last(byte bucket) throws IOException {
        return txnFinder.find(bucket, null,true);
    }

    private byte[] toRowKeyWithBucket(long txId, byte bucket) {
        byte[] result = TxnUtils.getRowKey(txId);
        assert result.length == 9 : "unexpected rowKey format";
        result[0] = bucket;
        return result;
    }

    // When we use a past transaction P whose timestamp is T, we can read data which is committed before T
    // i.e. all data with time < T will be visible, therefore, to be able to read the data of the past
    // transaction itself, we slide to the next tx.
    private long slideToNextTx(long tx) throws IOException {
        if(tx == -1) { // special case for timestamp that happened before the first transaction.
            return -1;
        }
        return tx + SIConstants.TRASANCTION_INCREMENT;
    }

    public Pair<Long, Long> getTxAt(long ts) throws IOException {
        long closestTS = Long.MAX_VALUE, closestTx = -1;
        Pair<Long, Long> before, after;
        for(byte i = 0; i < SIConstants.TRANSACTION_TABLE_BUCKET_COUNT; ++i) {
            before = first(i);
            if(before == null /*|| before.getBeginTs() > ts*/) {
                continue; // region is empty, or the earliest tx in there happened after 'ts'
            }
            if(before.getSecond() > ts) {
                if(LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG,"in bucket %d the first txn [%s (TS: %s), id %s] happened after the needle (%s) exiting!",
                            (int)i,
                            TxnUtils.rowKeytoHbaseEscaped(toRowKeyWithBucket(before.getFirst(), i)),
                            new Timestamp(before.getSecond()).toString(),
                            before.getFirst().toString(),
                            new Timestamp(ts).toString());
                    return new Pair<>(slideToNextTx(closestTx), closestTS); // nothing, needle is too early: <needle> ....... [before ...... after]
            }
            after = last(i);
            if(after.getSecond() < ts) {
                if(LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG,"in bucket %d the last txn [%s (TS: %s), id %s] happened before the needle (%s) exiting!",
                            (int)i,
                            TxnUtils.rowKeytoHbaseEscaped(toRowKeyWithBucket(after.getFirst(), i)),
                            new Timestamp(after.getSecond()).toString(),
                            after.getFirst().toString(),
                            new Timestamp(ts).toString());
                return new Pair<>(slideToNextTx(after.getFirst()), after.getSecond()); // after is the closest to our tx. [before ...... after] ........ <needle>
            }
            Pair<Long, Long> seachResults = bsearch(ts, i, toRowKeyWithBucket(before.getFirst(), i),
                    before.getSecond(), toRowKeyWithBucket(after.getFirst(), i), after.getSecond());
            if(seachResults == null) {
                continue;
            }
            if(Math.abs(seachResults.getSecond() - ts)< Math.abs(closestTS)) {
                closestTx = seachResults.getFirst();
                closestTS = seachResults.getSecond();
            }
        }
        return new Pair<>(slideToNextTx(closestTx), closestTS);
    }
}
