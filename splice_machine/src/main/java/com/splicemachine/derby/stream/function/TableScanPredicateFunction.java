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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.Scans;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 5/1/15.
 */
public class TableScanPredicateFunction<Op extends SpliceOperation> extends SplicePredicateFunction<Op,ExecRow> {
    protected boolean initialized;
    protected ScanOperation op;
    protected ExecutionFactory executionFactory;
    protected Qualifier[][] qualifiers;
    protected int[] baseColumnMap;
    protected boolean rowIdKey; // HACK Row ID Qualifiers point to the projection above them ?  TODO JL
    protected DataValueDescriptor optionalProbeValue;

    public TableScanPredicateFunction() {
        super();
    }

    public TableScanPredicateFunction(OperationContext<Op> operationContext) {
        this(operationContext,null);
    }

    public TableScanPredicateFunction(OperationContext<Op> operationContext, DataValueDescriptor optionalProbeValue) {
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
    public boolean apply(@Nullable ExecRow from) {
        try {
            if (!initialized) {
                initialized = true;
                op = getOperation() instanceof ScanOperation ? (ScanOperation) getOperation() : null;
                if (op != null) {
                    this.qualifiers = op.getScanInformation().getScanQualifiers();
                    this.baseColumnMap = op.getOperationInformation().getBaseColumnMap();
                    this.rowIdKey = op.getRowIdKey();
                }
            }
            if (qualifiers == null || rowIdKey || Scans.qualifyRecordFromRow(from.getRowArray(), qualifiers, baseColumnMap, optionalProbeValue)) {
                this.operationContext.recordRead();
                return true;
            }
            this.operationContext.recordFilter();
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
