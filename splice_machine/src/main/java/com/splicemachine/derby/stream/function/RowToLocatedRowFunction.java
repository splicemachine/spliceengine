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
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.spark.sql.Row;

import java.io.*;

import org.apache.spark.api.java.function.Function;

/**
 *
 * Allows a map to convert from RDD<LocatedRow> to RDD<Row>
 *
 */
public class RowToLocatedRowFunction implements Function <Row, LocatedRow>, Serializable, Externalizable {
    protected SpliceOperation op;
    protected ExecRow execRow;
    protected OperationContext operationContext;
    public RowToLocatedRowFunction() {
        super();
    }
    protected boolean initialized = false;

    public RowToLocatedRowFunction(OperationContext<SpliceOperation> operationContext) {
        this.operationContext = operationContext;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(operationContext);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        operationContext = (OperationContext) in.readObject();
    }
    @Override
    public LocatedRow call(Row row) throws Exception {
        if (!initialized) {
            op = operationContext.getOperation();
            execRow = op.getExecRowDefinition();
            initialized = true;
        }
        LocatedRow locatedRow = new LocatedRow(execRow.getClone().fromSparkRow(row));
        op.setCurrentLocatedRow(locatedRow);
        return locatedRow;
    }
}
