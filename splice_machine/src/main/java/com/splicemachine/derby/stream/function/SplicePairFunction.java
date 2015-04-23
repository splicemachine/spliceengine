package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import org.sparkproject.guava.common.base.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import javax.annotation.Nullable;

/**
 * Created by jleach on 4/15/15.
 */
public abstract class SplicePairFunction<Op extends SpliceOperation,K,V> implements PairFunction<V,K,V>, Function<V,K> {

    public abstract K internalCall(@Nullable V v);

    @Override
    public K apply(@Nullable V v) {
        return internalCall(v);
    }

    @Override
    public Tuple2<K, V> call(V v) throws Exception {
        return new Tuple2<K,V>(internalCall(v),v);
    }

}
