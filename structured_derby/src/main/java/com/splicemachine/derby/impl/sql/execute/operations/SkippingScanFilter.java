package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/22/14
 */
public class SkippingScanFilter extends FilterBase {
    private List<Pair<byte[], byte[]>> startStopKeys;
    private List<byte[]> predicates;

    private byte[] currentStartKey;
    private byte[] currentStopKey;
    private int currentIndex = 0;

    private boolean matchesRow;
    private boolean matchesEverything = false;

    //serialization filter, DO NOT USE
    @Deprecated
    public SkippingScanFilter() {
    }

    public SkippingScanFilter(List<Pair<byte[], byte[]>> startStopKeys, List<byte[]> predicates) {
        this.startStopKeys = startStopKeys;
        this.predicates = predicates;
    }

    @Override
    public void reset() {
        matchesRow = false;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
        //cheap skip if we are scanning everything
        if (matchesEverything) return ReturnCode.INCLUDE;
        //if we've already checked this row, then we don't need to do it again
        if (matchesRow) return ReturnCode.INCLUDE;

        byte[] kvBuffer = kv.getBuffer();
        int rowKeyOffset = kv.getRowOffset();
        short rowKeyLength = kv.getRowLength();
        if (currentStartKey.length <= 0) {
            if (Bytes.compareTo(currentStopKey, 0, currentStopKey.length, kvBuffer, rowKeyOffset, rowKeyLength) <= 0) {
                //this kv is past the end of this current value. set the next range and try again
                Pair<byte[], byte[]> newRange = startStopKeys.get(currentIndex++);
                currentStartKey = newRange.getFirst();
                currentStopKey = newRange.getSecond();
                return filterKeyValue(kv); //try the looping again
            }
            //we are contained in this range, so include it
            return ReturnCode.INCLUDE;
        } else if (Bytes.compareTo(currentStartKey, 0, currentStartKey.length, kvBuffer, rowKeyOffset, rowKeyLength) > 0) {
            //the current start key is after this kv, so seek to the current start key
            return ReturnCode.SEEK_NEXT_USING_HINT;
        } else if (Bytes.compareTo(currentStopKey, 0, currentStopKey.length, kvBuffer, rowKeyOffset, rowKeyLength) <= 0) {
            //this kv is past the end of this current value. set the next range and try again
            Pair<byte[], byte[]> newRange = startStopKeys.get(currentIndex++);
            currentStartKey = newRange.getFirst();
            currentStopKey = newRange.getSecond();
            return filterKeyValue(kv); //try the looping again
        } else {
            matchesRow = true; //don't bother processing future items
            return ReturnCode.INCLUDE;
        }
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue currentKV) {
        return new KeyValue(currentStartKey, SpliceConstants.DEFAULT_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(startStopKeys.size());
        for (int i = 0; i < startStopKeys.size(); i++) {
            Pair<byte[], byte[]> startStopKey = startStopKeys.get(i);
            byte[] start = startStopKey.getFirst();
            out.writeInt(start.length);
            out.write(start);
            byte[] stop = startStopKey.getSecond();
            out.writeInt(stop.length);
            out.write(stop);

            byte[] predicate = predicates.get(i);
            out.writeInt(predicate.length);
            out.write(predicate);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int startStopSize = in.readInt();
        startStopKeys = Lists.newArrayListWithCapacity(startStopSize);
        predicates = Lists.newArrayListWithCapacity(startStopSize);
        for (int i = 0; i < startStopSize; i++) {
            byte[] start = new byte[in.readInt()];
            in.readFully(start);
            byte[] stop = new byte[in.readInt()];
            in.readFully(stop);
            startStopKeys.add(Pair.newPair(start, stop));
            byte[] predicate = new byte[in.readInt()];
            in.readFully(predicate);
            predicates.add(predicate);
        }

        Pair<byte[], byte[]> first = startStopKeys.get(currentIndex++);
        currentStartKey = first.getFirst();
        currentStopKey = first.getSecond();

        matchesEverything = currentStartKey.length <= 0 && currentStopKey.length <= 0;
    }

    public ObjectArrayList<Predicate> getNextPredicates(KeyValue kv) throws IOException {
        for (int i = 0; i < startStopKeys.size(); i++) {
            Pair<byte[], byte[]> range = startStopKeys.get(i);
            if (BytesUtil.isKeyValueInRange(kv, range)) {
                return EntryPredicateFilter.fromBytes(predicates.get(i)).getValuePredicates();
            }
        }
        /* No predicates for this KeyValue, return empty predicate list. */
        ObjectArrayList<Predicate> EMPTY = EntryPredicateFilter.EMPTY_PREDICATE.getValuePredicates();
        /* Returning a mutable constant (optimization) here, assumes caller will not modify! */
        Preconditions.checkState(EMPTY.isEmpty());
        return EMPTY;
    }

    public List<Pair<byte[], byte[]>> getStartStopKeys() {
        return startStopKeys;
    }

    public List<byte[]> getPredicates() {
        return predicates;
    }
}
