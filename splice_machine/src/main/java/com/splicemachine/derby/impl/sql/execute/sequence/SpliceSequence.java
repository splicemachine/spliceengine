package com.splicemachine.derby.impl.sql.execute.sequence;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.Encoding;

public class SpliceSequence extends AbstractSequence {
	protected byte[] sysColumnsRow;
    protected static final byte[] autoIncrementValueQualifier = Encoding.encode(7);

    public SpliceSequence() {
        super();
    }

	public SpliceSequence (long blockAllocationSize,byte[] sysColumnsRow,
            long startingValue,
            long incrementSteps) {
			super(blockAllocationSize,incrementSteps, startingValue);
			this.sysColumnsRow = sysColumnsRow;
}

	@Override
	protected long getCurrentValue() throws IOException {
        Table sysColumnTable = null;
        try {
            sysColumnTable = SpliceAccessManager.getHTable(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES);
            Get currValue = new Get(sysColumnsRow);
            currValue.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, autoIncrementValueQualifier);
            Result result = sysColumnTable.get(currValue);
            if (result == null || result.isEmpty())
                return startingValue;
            return Encoding.decodeLong(result.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, autoIncrementValueQualifier));
        } finally {
            if (sysColumnTable != null)
                sysColumnTable.close();
        }
	}

	@Override
	protected boolean atomicIncrement(long next) throws IOException {
        Table sysColumnTable = null;
        try {
            sysColumnTable = SpliceAccessManager.getHTable(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES);
            Put put = new Put(sysColumnsRow);
            put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, autoIncrementValueQualifier, Encoding.encode(next));
            return sysColumnTable.checkAndPut(sysColumnsRow,
                    SpliceConstants.DEFAULT_FAMILY_BYTES,
                    autoIncrementValueQualifier, currPosition.get() == startingValue ? null : Encoding.encode(currPosition.get()), put);
        } finally {
            if (sysColumnTable != null)
                sysColumnTable.close();
        }
	}

	@Override
	public void close() throws IOException {
        // No Op
	}


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(sysColumnsRow.length);
        out.write(sysColumnsRow);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        sysColumnsRow = new byte[in.readInt()];
        in.read(sysColumnsRow);
    }
}
