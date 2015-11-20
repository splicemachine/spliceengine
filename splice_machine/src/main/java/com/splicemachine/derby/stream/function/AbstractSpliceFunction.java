package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.*;

import org.apache.commons.lang3.StringUtils;

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

    public String SPARK_NAME_DELIM = ": ";

    public String getPrettyFunctionName() {
        return StringUtils.join(
            StringUtils.splitByCharacterTypeCamelCase(this.getClass().getSimpleName().replace("Function", "")),
            ' '
        );
        // String[] words = this.getClass().getSimpleName().replace("Function", "").split("(?=[A-Z])");
        // return StringUtils.join(words, " ");
    }
    
    public String getPrettyFunctionDesc() {
        return "";
    }
    
    public String getSparkName() {
        return getPrettyFunctionName() +
            (getPrettyFunctionDesc() != null && !getPrettyFunctionDesc().isEmpty() ?
            SPARK_NAME_DELIM + getPrettyFunctionDesc() : "");
    }
    
}
