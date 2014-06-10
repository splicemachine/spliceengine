package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.storage.BaseHashAwareScanBoundary;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;

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
				/*
				 * We use 18 instead of 17(annoying magic number) because we want to skip
			   * the postfix field separator byte
				 */
				int ordinalOffset = data.length-18;
				return BytesUtil.slice(data,0,ordinalOffset);
    }
}
