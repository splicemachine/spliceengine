package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.SpliceZeroCopyByteString;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.hbase.AbstractSkippingScanFilter;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/22/14
 */
public class SkippingScanFilter extends AbstractSkippingScanFilter<Cell> {
    public SkippingScanFilter() {
    	super();
    }

    public SkippingScanFilter(List<Pair<byte[], byte[]>> startStopKeys, List<byte[]> predicates) {
    	super(startStopKeys,predicates);
    }


    @Override
    public ReturnCode filterKeyValue(Cell kv) {
    	return internalFilter(kv);
    }
    @Override
    public byte[] toByteArray() throws IOException {
        SpliceMessage.SkippingScanFilterMessage.Builder builder = SpliceMessage.SkippingScanFilterMessage.newBuilder();
        for(byte[] predicate : predicates){
            builder.addPredicates(SpliceZeroCopyByteString.wrap(predicate));
        }

        for(Pair<byte[],byte[]> startStop : startStopKeys){
            builder.addStartKeys(SpliceZeroCopyByteString.wrap(startStop.getFirst()));
            builder.addStopKeys(SpliceZeroCopyByteString.wrap(startStop.getSecond()));
        }
        return builder.build().toByteArray();
    }

    public static SkippingScanFilter parseFrom(byte[] bytes) throws DeserializationException{
        SpliceMessage.SkippingScanFilterMessage proto;
        try {
            proto= SpliceMessage.SkippingScanFilterMessage.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }

        List<byte[]> predicates = Lists.transform(proto.getPredicatesList(),new Function<ByteString, byte[]>() {
            @Override
            public byte[] apply(ByteString input) {
                return input.toByteArray();
            }
        });

        List<ByteString> starts = proto.getStartKeysList();
        List<ByteString> stops = proto.getStopKeysList();
        List<Pair<byte[],byte[]>> startStopKeys = Lists.newArrayListWithCapacity(starts.size());
        for(int i=0;i<starts.size();i++){
            startStopKeys.add(Pair.newPair(starts.get(i).toByteArray(),stops.get(i).toByteArray()));
        }
        SkippingScanFilter skippingScanFilter = new SkippingScanFilter(startStopKeys, predicates);
        skippingScanFilter.adjustStartStopKeys();
        skippingScanFilter.matchesEverything = skippingScanFilter.currentStartKey.length<=0 && skippingScanFilter.currentStopKey.length<=0;
        return skippingScanFilter;
    }
    
    public boolean adjustStartStopKeys() {
        if(currentIndex <=  startStopKeys.size() - 1){
            //this kv is past the end of this current value. set the next range and try again
            Pair<byte[],byte[]> newRange = startStopKeys.get(currentIndex);
            currentStartKey = newRange.getFirst();
            currentStopKey = newRange.getSecond();
            currentIndex++;
            return true;
        }else{
            currentStartKey = null;
            currentStopKey =null;
            return false;
        }
    }
}
