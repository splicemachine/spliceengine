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
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.api.data.TxnOperationFactory;
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
