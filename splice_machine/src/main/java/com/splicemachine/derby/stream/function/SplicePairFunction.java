package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

/**
 * Created by jleach on 4/15/15.
 */
public abstract class SplicePairFunction<Op extends SpliceOperation,V,K,U> extends AbstractSpliceFunction<Op> implements SplittingFunction<V,K,U> {

    public SplicePairFunction() {
        super();
    }

    public SplicePairFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    public abstract K genKey(V v);

    public abstract U genValue(V v);

}
