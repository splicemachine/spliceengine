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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.Scans;
import scala.Tuple2;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;

/**
 * Created by jleach on 4/20/15.
 */
public class TableScanTupleFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op,Tuple2<RowLocation,ExecRow>,LocatedRow> implements Serializable {
    protected boolean initialized;
    protected ScanOperation op;
    protected ExecutionFactory executionFactory;
    protected Qualifier[][] qualifiers;
    protected int[] baseColumnMap;
    protected boolean rowIdKey; // HACK Row ID Qualifiers point to the projection above them ?  TODO JL
    protected DataValueDescriptor optionalProbeValue;


    public TableScanTupleFunction() {
        super();
    }

    public TableScanTupleFunction(OperationContext<Op> operationContext, DataValueDescriptor optionalProbeValue) {
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
    public Iterable<LocatedRow> call(Tuple2<RowLocation,ExecRow> from) throws Exception {
        if (!initialized) {
            initialized = true;
            op = (ScanOperation) getOperation();
            if (op != null) {
                this.qualifiers = op.getScanInformation().getScanQualifiers();
                this.baseColumnMap = op.getOperationInformation().getBaseColumnMap();
                this.rowIdKey = op.getRowIdKey();
            }
        }
        if (qualifiers == null || rowIdKey || Scans.qualifyRecordFromRow(from._2().getRowArray(), qualifiers,baseColumnMap,optionalProbeValue)) {
            LocatedRow locatedRow = new LocatedRow(from._1(), from._2());
            this.operationContext.recordRead();
            if (op!=null)
                op.setCurrentLocatedRow(locatedRow);
            return Collections.singletonList(locatedRow);
        }
        this.operationContext.recordFilter();
        return Collections.emptyList();
    }
}