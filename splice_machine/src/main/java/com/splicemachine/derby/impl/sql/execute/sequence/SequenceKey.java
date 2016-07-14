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
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.storage.Partition;
import com.splicemachine.tools.ResourcePool;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

public class SequenceKey implements ResourcePool.Key{
    private final PartitionFactory partitionFactory;
    private final TxnOperationFactory opFactory;
    protected final byte[] sysColumnsRow;
    protected final long blockAllocationSize;
    protected long autoIncStart;
    protected long autoIncrement;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public SequenceKey(
            byte[] sysColumnsRow,
            long blockAllocationSize,
            long autoIncStart,
            long autoIncrement,
            PartitionFactory partitionFactory,
            TxnOperationFactory opFactory){
        assert (sysColumnsRow!=null):"Null SysColumnsRow Passed in";
        this.partitionFactory = partitionFactory;
        this.opFactory = opFactory;
        this.sysColumnsRow=sysColumnsRow;
        this.blockAllocationSize=blockAllocationSize;
        this.autoIncStart=autoIncStart;
        this.autoIncrement=autoIncrement;
    }

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof SequenceKey)) return false;
        SequenceKey key=(SequenceKey)o;
        return Arrays.equals(sysColumnsRow,key.sysColumnsRow)
                && blockAllocationSize==key.blockAllocationSize &&
                autoIncStart==key.autoIncStart &&
                autoIncrement==key.autoIncrement;
    }

    @Override
    public int hashCode(){
        return Arrays.hashCode(sysColumnsRow);
    }

    public long getIncrementSize() throws StandardException{
        return autoIncrement;
    }

    public SpliceSequence makeNew() throws StandardException{
        return new SpliceSequence(blockAllocationSize,sysColumnsRow,
                autoIncStart, autoIncrement,partitionFactory,opFactory);
    }
}