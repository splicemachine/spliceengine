package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.spark.api.java.function.Function;
import org.sparkproject.guava.common.base.Predicate;

/**
 * Created by jleach on 4/22/15.
 */
public abstract class SplicePredicateFunction<Op extends SpliceOperation, From>
        extends AbstractSpliceFunction<Op>
        implements Function<From, Boolean>, Predicate<From> {

    public SplicePredicateFunction() {
        super();
    }
    public SplicePredicateFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Boolean call(From from) throws Exception {
        return apply(from);
    }


}