/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.Scans;
import org.apache.commons.collections.iterators.SingletonIterator;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;

/**
 * This function applies table scan qualifiers to each row.
 *
 */

public class TableScanQualifierFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op,LocatedRow,LocatedRow> implements Serializable {
    protected boolean initialized;
    protected ScanOperation op;
    protected ExecutionFactory executionFactory;
    protected Qualifier[][] qualifiers;
    protected int[] baseColumnMap;
    protected boolean rowIdKey; // HACK Row ID Qualifiers point to the projection above them ?  TODO JL
    protected DataValueDescriptor optionalProbeValue;

    public TableScanQualifierFunction() {
        super();
    }

    public TableScanQualifierFunction(OperationContext<Op> operationContext, DataValueDescriptor optionalProbeValue) {
        super(operationContext);
        this.optionalProbeValue = optionalProbeValue;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(optionalProbeValue!=null);
        if (optionalProbeValue!=null)
            out.writeObject(optionalProbeValue);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        if (in.readBoolean())
            optionalProbeValue = (DataValueDescriptor) in.readObject();
    }

    @Override
    public Iterator<LocatedRow> call(LocatedRow from) throws Exception {
        if (!initialized) {
            initialized = true;
            op = (ScanOperation) getOperation();
            if (op != null) {
                this.qualifiers = op.getScanInformation().getScanQualifiers();
                this.baseColumnMap = op.getOperationInformation().getBaseColumnMap();
                this.rowIdKey = op.getRowIdKey();
            }
        }
        if (qualifiers == null || rowIdKey || Scans.qualifyRecordFromRow(from.getRow().getRowArray(), qualifiers,baseColumnMap,optionalProbeValue)) {
            this.operationContext.recordRead();
            LocatedRow locatedRow = new LocatedRow(from.getRow());
            if (op!=null)
                op.setCurrentLocatedRow(locatedRow);
            return new SingletonIterator(locatedRow);
        }
        this.operationContext.recordFilter();
        return Collections.<LocatedRow>emptyList().iterator();
    }
}
