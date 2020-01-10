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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.utils.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.*;

/**
 * Created by jleach on 4/17/15.
 */
public abstract class AbstractSpliceFunction<Op extends SpliceOperation> implements Externalizable, Serializable {
    public OperationContext<Op> operationContext;
    public AbstractSpliceFunction() {

    }

    public AbstractSpliceFunction(OperationContext<Op> operationContext) {
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

    public Op getOperation() {
        return operationContext.getOperation();
    }

    public Activation getActivation() {
        return operationContext.getActivation();
    }

    public void prepare() {
        operationContext.prepare();
    }

    public void reset() {
        operationContext.reset();
    }

    public String getPrettyFunctionName() {
        return StreamUtils.getPrettyFunctionName(this.getClass().getSimpleName());
    }
    
    public String getSparkName() {
        return getPrettyFunctionName();
    }

    public ExecRow getExecRow() throws StandardException {
        return operationContext.getOperation().getExecRowDefinition();
    }

    public boolean hasNativeSparkImplementation() {
        return false;
    }

    public Pair<Dataset<Row>, OperationContext> nativeTransformation(Dataset<Row> input, OperationContext context) {
        return Pair.newPair(input, null);
    }
}
