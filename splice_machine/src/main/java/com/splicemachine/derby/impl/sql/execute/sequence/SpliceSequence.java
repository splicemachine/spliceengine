/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.configuration.OperationConfiguration;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SpliceSequence extends AbstractSequence{
    protected byte[] sysColumnsRow;
    private PartitionFactory partitionFactory;
    private TxnOperationFactory opFactory;

    public SpliceSequence(){
        super();
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
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
        throw new UnsupportedOperationException("Not Implemented yet");
        /*
        try(Partition sysColumnTable = partitionFactory.getTable(OperationConfiguration.SEQUENCE_TABLE_NAME_BYTES)){
            sysColumnTable.increment(sysColumnsRow)
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
        */
    }

    @Override
    protected boolean atomicIncrement(long next) throws IOException{
        try(Partition sysColumnTable = partitionFactory.getTable(OperationConfiguration.SEQUENCE_TABLE_NAME_BYTES)) {
             sysColumnTable.increment(sysColumnsRow, next);
        }
        return true;
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
        int size=in.readInt();
        sysColumnsRow=new byte[size];
        in.readFully(sysColumnsRow);
    }
}
