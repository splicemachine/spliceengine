package com.splicemachine.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/22/14
 */
public abstract class AbstractSkippingScanFilter<Data> extends FilterBase implements Writable {
	private static final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
    protected List<Pair<byte[], byte[]>> startStopKeys;
    protected List<byte[]> predicates;

    protected byte[] currentStartKey;
    protected byte[] currentStopKey;
    protected int currentIndex;

    private boolean matchesRow;
    protected boolean matchesEverything;

    //serialization filter, DO NOT USE
    @Deprecated
    public AbstractSkippingScanFilter() {
    }

    public AbstractSkippingScanFilter(List<Pair<byte[], byte[]>> startStopKeys, List<byte[]> predicates) {
        this.startStopKeys = startStopKeys;
        this.predicates = predicates;
    }

    @Override
    public void reset() {
        matchesRow = false;
    }

    public ReturnCode internalFilter(Data kv) {
        //cheap skip if we are scanning everything
        if (matchesEverything) return ReturnCode.INCLUDE;
        //if we've already checked this row, then we don't need to do it again
        if (matchesRow) return ReturnCode.INCLUDE;

        byte[] kvBuffer = dataLib.getDataRowBuffer(kv);
        int rowKeyOffset = dataLib.getDataRowOffset(kv);
        int rowKeyLength = dataLib.getDataRowlength(kv);
        if (currentStartKey.length == 0) {
            if (Bytes.compareTo(currentStopKey, 0, currentStopKey.length, kvBuffer, rowKeyOffset, rowKeyLength) <= 0) {
                //this kv is past the end of this current range. set the next range and try again
                Pair<byte[], byte[]> newRange = startStopKeys.get(currentIndex++);
                currentStartKey = newRange.getFirst();
                currentStopKey = newRange.getSecond();
                return internalFilter(kv); //try the looping again
            }
            //we are contained in this range, so include it
            return ReturnCode.INCLUDE;
        } else if (Bytes.compareTo(currentStartKey, 0, currentStartKey.length, kvBuffer, rowKeyOffset, rowKeyLength) > 0) {
            //the current start key is after this kv, so seek to the current start key
            return ReturnCode.SEEK_NEXT_USING_HINT;
        } else if (Bytes.compareTo(currentStopKey, 0, currentStopKey.length, kvBuffer, rowKeyOffset, rowKeyLength) <= 0) {
            //this kv is past the end of this current range. set the next range and try again
            Pair<byte[], byte[]> newRange = startStopKeys.get(currentIndex++);
            currentStartKey = newRange.getFirst();
            currentStopKey = newRange.getSecond();
            return internalFilter(kv); //try the looping again
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
    	try {
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
    	} catch (IOException e) {
    		throw e;
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

        matchesEverything = currentStartKey.length == 0 && currentStopKey.length == 0;
    }

    public List<Pair<byte[], byte[]>> getStartStopKeys() {
        return startStopKeys;
    }

    public List<byte[]> getPredicates() {
        return predicates;
    }
}
