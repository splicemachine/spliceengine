package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.storage.BaseHashAwareScanBoundary;
import org.apache.hadoop.hbase.client.Result;

/**
 * @author Scott Fines
 *         Created on: 10/30/13
 */
public class MergeSortScanBoundary extends BaseHashAwareScanBoundary{
		private final int prefixOffset;

    public MergeSortScanBoundary(byte[] columnFamily, int prefixOffset) {
        super(columnFamily);
				this.prefixOffset = prefixOffset;
    }

    @Override
    public byte[] getStartKey(Result result) {
        return getKey(result);
    }

    @Override
    public byte[] getStopKey(Result result) {
        byte[] startKey = getKey(result);
        BytesUtil.unsignedIncrement(startKey,startKey.length-1);
        return startKey;
    }

    private byte[] getKey(Result result) {
        byte[] data = result.getRow();
        if(data==null) return null;
				int ordinalOffset = data.length-17;
				return BytesUtil.slice(data,0,ordinalOffset);
    }
}
