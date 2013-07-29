package com.splicemachine.derby.impl.storage;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.hadoop.hbase.client.Result;

/**
 * @author Scott Fines
 *         Created on: 6/18/13
 */
public class SingleTypeHashAwareScanBoundary2 extends BaseHashAwareScanBoundary{
    protected RowDecoder decoder;
    protected int slice;
    
    public SingleTypeHashAwareScanBoundary2(byte[] columnFamily,RowDecoder decoder) {
        this(columnFamily,decoder,decoder.getKeyColumns().length);
    }

    public SingleTypeHashAwareScanBoundary2(byte[] columnFamily,RowDecoder decoder, int slice) {
        super(columnFamily);
        this.decoder = decoder;
        this.slice = slice;
    }

    @Override
    public byte[] getStartKey(Result result) {
        MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(result.getRow());
        fieldDecoder.seek(9); //skip the prefix value
        return fieldDecoder.slice(slice);
    }

    @Override
    public byte[] getStopKey(Result result) {
        byte[] start = getStartKey(result);
        BytesUtil.unsignedIncrement(start,start.length-1);
        return start;
    }
}
