package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.sparkproject.guava.common.base.Function;
import javax.annotation.Nullable;

/**
 * Created by dgomezferro on 4/4/14.
 */
public abstract class SpliceFlatMapFunction<Op extends SpliceOperation, From, To>
		extends AbstractSpliceFunction<Op> implements FlatMapFunction<From, To>, Function<From,Iterable<To>> {
	public SpliceFlatMapFunction() {
	}

	public SpliceFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
	}

    @Nullable
    @Override
    public Iterable<To> apply(@Nullable From  from) {
        try {
            return call(from);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}