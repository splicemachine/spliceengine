package com.splicemachine.derby.stream.spark;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import scala.Tuple2;

/**
 * Created by jleach on 6/20/17.
 */
public class EmptySparkPairDataSet<V> extends SplicePairFunction<SpliceOperation, V, Object, Object> {

    public EmptySparkPairDataSet() {}

    @Override
    public Object genKey(V v) {
        return null;
    }

    @Override
    public Object genValue(V v) {
        return v;
    }

    @Override
    public Tuple2<Object, Object> call(V value) throws Exception {
        return new Tuple2<Object, Object>(null,value);
    }
}
