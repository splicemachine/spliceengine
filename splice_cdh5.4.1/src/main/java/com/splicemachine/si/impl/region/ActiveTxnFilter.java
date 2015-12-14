package com.splicemachine.si.impl.region;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 8/18/14
 */
public class ActiveTxnFilter extends FilterBase implements Writable {
    BaseActiveTxnFilter<OperationWithAttributes,Cell,Delete,Filter,Get,
            Put,RegionScanner,Result,ReturnCode,Scan> baseActiveTxnFilter;
    public ActiveTxnFilter(long beforeTs, long afterTs, byte[] destinationTable) {
        baseActiveTxnFilter = new BaseActiveTxnFilter(beforeTs,afterTs,destinationTable);
    }
    
    @Override
    public ReturnCode filterKeyValue(Cell kv) {
    	return baseActiveTxnFilter.internalFilter(kv);
    }
    
    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        return baseActiveTxnFilter.filterRowKey(buffer, offset, length);
    }

    @Override
    public boolean filterRow() {
        return baseActiveTxnFilter.filterRow();
    }

    @Override
    public void reset() {
        baseActiveTxnFilter.reset();
    }


    @Override public void write(DataOutput out) throws IOException {  }
    @Override public void readFields(DataInput in) throws IOException {  }
}