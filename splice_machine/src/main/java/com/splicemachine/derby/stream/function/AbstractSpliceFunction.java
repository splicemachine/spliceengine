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
