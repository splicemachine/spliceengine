package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.storage.BaseHashAwareScanBoundary;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.hadoop.hbase.client.Result;

/**
 * @author Scott Fines
 *         Created on: 10/30/13
 */
public class MergeSortScanBoundary extends BaseHashAwareScanBoundary{
    private final RowDecoder leftDecoder;
    private final RowDecoder rightDecoder;

    public MergeSortScanBoundary(byte[] columnFamily,RowDecoder leftDecoder,RowDecoder rightDecoder) {
        super(columnFamily);
        this.leftDecoder = leftDecoder;
        this.rightDecoder = rightDecoder;
    }

    @Override
    public byte[] getStartKey(Result result) {
        byte[] data = result.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, JoinUtils.JOIN_SIDE_COLUMN);
        if(data==null) return null;
        int ordinal = Encoding.decodeInt(data);
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(result.getRow(), SpliceDriver.getKryoPool());
        decoder.seek(9); //skip the prefix value
        if(ordinal== JoinUtils.JoinSide.RIGHT.ordinal()){
            //copy out all the fields from the key until we reach the ordinal
            return decoder.slice(rightDecoder.getKeyColumns().length);
        }else{
            return decoder.slice(leftDecoder.getKeyColumns().length);
        }
    }

    @Override
    public byte[] getStopKey(Result result) {
        byte[] data = result.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, JoinUtils.JOIN_SIDE_COLUMN);
        if(data==null) return null;
        int ordinal = Encoding.decodeInt(data);
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(result.getRow(),SpliceDriver.getKryoPool());
        decoder.seek(9); //skip the prefix value
        if(ordinal== JoinUtils.JoinSide.RIGHT.ordinal()){
            //copy out all the fields from the key until we reach the ordinal
            return decoder.slice(rightDecoder.getKeyColumns().length);
        }else{
            return decoder.slice(leftDecoder.getKeyColumns().length);
        }
    }
}
