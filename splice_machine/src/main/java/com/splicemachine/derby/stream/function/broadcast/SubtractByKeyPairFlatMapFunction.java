package com.splicemachine.derby.stream.function.broadcast;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * Created by jleach on 5/27/15.
 */
public class SubtractByKeyPairFlatMapFunction<K,V> implements PairFlatMapFunction<Tuple2<K, V>, K, V>, Serializable {
    protected Broadcast<Set<K>> broadcast;
    public SubtractByKeyPairFlatMapFunction() {}

    public SubtractByKeyPairFlatMapFunction(Broadcast<Set<K>> broadcast) {
        this.broadcast = broadcast;
    }
    @Override
    public Iterable<Tuple2<K, V>> call(Tuple2<K, V> tuple) throws Exception {
        Set<K> rightMap = broadcast.value();
        if (rightMap.contains(tuple._1))
            return Collections.EMPTY_LIST;
        return Collections.singletonList(tuple);
    }
}