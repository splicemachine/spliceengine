package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.derby.impl.sql.execute.operations.OperationConfiguration;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SpliceSequence extends AbstractSequence{
    protected byte[] sysColumnsRow;
    protected static final byte[] autoIncrementValueQualifier=Encoding.encode(7);
    private PartitionFactory partitionFactory;
    private TxnOperationFactory opFactory;

    public SpliceSequence(){
        super();
    }

    public SpliceSequence(long blockAllocationSize,byte[] sysColumnsRow,
                          long startingValue,
                          long incrementSteps,
                          PartitionFactory partitionFactory,
                          TxnOperationFactory operationFactory){
        super(blockAllocationSize,incrementSteps,startingValue);
        this.sysColumnsRow=sysColumnsRow;
        this.partitionFactory = partitionFactory;
        this.opFactory = operationFactory;
    }

    @Override
    protected long getCurrentValue() throws IOException{
        try(Partition sysColumnTable = partitionFactory.getTable(OperationConfiguration.SEQUENCE_TABLE_NAME_BYTES)){
            DataGet currValue=opFactory.newDataGet(null,sysColumnsRow,null);
            currValue.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,autoIncrementValueQualifier);
            currValue.returnAllVersions();
            currValue.setTimeRange(0l,Long.MAX_VALUE);
            DataResult result=sysColumnTable.get(currValue,null);
            if(result==null || result.size()<=0)
                return startingValue;
            DataCell dataCell=result.latestCell(SIConstants.DEFAULT_FAMILY_BYTES,autoIncrementValueQualifier);
            return Encoding.decodeLong(dataCell.valueArray(),dataCell.valueOffset(),false);
        }
    }

    @Override
    protected boolean atomicIncrement(long next) throws IOException{
        try(Partition sysColumnTable = partitionFactory.getTable(OperationConfiguration.SEQUENCE_TABLE_NAME_BYTES)){
            DataPut put=opFactory.newDataPut(null,sysColumnsRow);
            put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,autoIncrementValueQualifier,Encoding.encode(next));
            return sysColumnTable.checkAndPut(sysColumnsRow,
                    SIConstants.DEFAULT_FAMILY_BYTES,
                    autoIncrementValueQualifier,currPosition.get()==startingValue?null:Encoding.encode(currPosition.get()),put);
        }
    }

    @Override
    public void close() throws IOException{
        // No Op
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeInt(sysColumnsRow.length);
        out.write(sysColumnsRow);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        sysColumnsRow=new byte[in.readInt()];
        in.read(sysColumnsRow);
    }
}
