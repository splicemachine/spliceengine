package com.splicemachine.derby.impl.sql.execute.sequence;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.Encoding;

public class SpliceSequence extends AbstractSequence {
	protected HTableInterface sysColumns;
	protected byte[] sysColumnsRow;
    protected static final byte[] autoIncrementValueQualifier = Encoding.encode(7);

	
	public SpliceSequence (HTableInterface sysColumns,
            long blockAllocationSize,byte[] sysColumnsRow,
            long startingValue,
            long incrementSteps) {
			super(blockAllocationSize,incrementSteps, startingValue);
			this.sysColumns = sysColumns;
			this.sysColumnsRow = sysColumnsRow;
}

	@Override
	protected long getCurrentValue() throws IOException {
        Get currValue = new Get(sysColumnsRow);
        currValue.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,autoIncrementValueQualifier);
        Result result = sysColumns.get(currValue);
        if(result==null||result.isEmpty())
            return startingValue;
        return Encoding.decodeLong(result.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES,autoIncrementValueQualifier));
	}

	@Override
	protected boolean atomicIncrement(long next) throws IOException {
        Put put = new Put(sysColumnsRow);
        put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,autoIncrementValueQualifier,Encoding.encode(next));
        return sysColumns.checkAndPut(sysColumnsRow,
                SpliceConstants.DEFAULT_FAMILY_BYTES,
                autoIncrementValueQualifier,Encoding.encode(currPosition.get()),put);
	}

	@Override
	public void close() throws IOException {
	       sysColumns.close();
	}


}
