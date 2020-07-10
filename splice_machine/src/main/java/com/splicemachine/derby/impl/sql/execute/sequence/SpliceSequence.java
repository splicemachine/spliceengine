/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.configuration.OperationConfiguration;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SpliceSequence extends AbstractSequence{
    protected byte[] sysColumnsRow;
    static final byte[] autoIncrementValueQualifier=Encoding.encode(7);
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
        try(Partition sysColumnTable = partitionFactory.getTable(OperationConfiguration.SEQUENCE_TABLE_NAME_BYTES)){
            DataGet currValue=opFactory.newDataGet(null,sysColumnsRow,null);
            currValue.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,autoIncrementValueQualifier);
            currValue.returnLatestVersion();
            currValue.setTimeRange(0l,Long.MAX_VALUE);
            DataResult result=sysColumnTable.get(currValue,null);
            if(result==null || result.isEmpty())
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
        int size=in.readInt();
        sysColumnsRow=new byte[size];
        in.readFully(sysColumnsRow);
    }
}
