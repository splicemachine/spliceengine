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

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.StreamUtils;

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
}
