package com.splicemachine.derby.stream.function;

import com.google.common.base.Optional;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by jleach on 5/27/15.
 */
public abstract class SplicePairFlatMapFunction<Op extends SpliceOperation, T,K,V> extends AbstractSpliceFunction<Op> implements PairFlatMapFunction<T,K,V>, Serializable {

    public SplicePairFlatMapFunction() {}

    public SplicePairFlatMapFunction(OperationContext operationContext) {
        super(operationContext);
    }

}
