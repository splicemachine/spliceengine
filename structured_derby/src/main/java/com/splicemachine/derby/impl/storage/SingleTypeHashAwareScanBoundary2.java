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

    public SingleTypeHashAwareScanBoundary2(byte[] columnFamily,
                                            RowDecoder decoder) {
        super(columnFamily);
        this.decoder = decoder;
    }

    @Override
    public byte[] getStartKey(Result result) {
        MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(result.getRow());
        fieldDecoder.skip(); //skip the prefix value
        return fieldDecoder.slice(decoder.getKeyColumns().length);
    }

    @Override
    public byte[] getStopKey(Result result) {
        byte[] start = getStartKey(result);
        BytesUtil.incrementAtIndex(start,start.length-1);
        return start;
    }
}
