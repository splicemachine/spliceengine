package com.splicemachine.si.impl.region;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.SpliceZeroCopyByteString;
import com.splicemachine.coprocessor.SpliceMessage;
import org.apache.hadoop.hbase.Cell;

import com.splicemachine.si.impl.TxnUtils;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 8/18/14
 */
public class ActiveTxnFilter extends BaseActiveTxnFilter<Cell> {
	
    public ActiveTxnFilter(long beforeTs, long afterTs, byte[] destinationTable) {
    	super(beforeTs,afterTs,destinationTable);
    }
    
    @Override
    public ReturnCode filterKeyValue(Cell kv) {
    	return this.internalFilter(kv);
    }
    
    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
				/*
				 * Since the transaction id must necessarily be the begin timestamp in both
				 * the old and new format, we can filter transactions out based entirely on the
				 * row key here
				 */
        if (length == 0) return false;
        long txnId = TxnUtils.txnIdFromRowKey(buffer, offset, length);    	
        boolean withinRange = txnId >= afterTs && txnId <= beforeTs;
        return !withinRange;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        SpliceMessage.ActiveTxnFilterMessage.Builder builder = SpliceMessage.ActiveTxnFilterMessage.newBuilder();
        builder.setAfterTs(afterTs);
        builder.setBeforeTs(beforeTs);
        if (destinationTable != null) {
            builder.setDestinationTable(SpliceZeroCopyByteString.wrap(destinationTable));
        }
        return builder.build().toByteArray();
    }

    public static ActiveTxnFilter parseFrom(byte[] bytes) throws DeserializationException {
        SpliceMessage.ActiveTxnFilterMessage proto;
        try {
            proto = SpliceMessage.ActiveTxnFilterMessage.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }

        return new ActiveTxnFilter(proto.getBeforeTs(), proto.getAfterTs(), proto.getDestinationTable().toByteArray());
    }

}