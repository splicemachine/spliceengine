package com.splicemachine.derby.stream.function.broadcast;

import com.google.common.base.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.sparkproject.guava.common.collect.Lists;
import org.sparkproject.guava.common.collect.Multimap;
import scala.Tuple2;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Created by jleach on 5/27/15.
 */
public class RightOuterJoinPairFlatMapFunction<K,V,W> implements PairFlatMapFunction<Tuple2<K, W>, K, Tuple2<Optional<V>, W>>, Serializable {
    protected Broadcast<Multimap<K,V>> broadcast;
    public RightOuterJoinPairFlatMapFunction() {}

    public RightOuterJoinPairFlatMapFunction(Broadcast<Multimap<K, V>> broadcast) {
        this.broadcast = broadcast;
    }

    @Override
    public Iterable<Tuple2<K, Tuple2<Optional<V>, W>>> call(Tuple2<K, W> tuple) throws Exception {
        Multimap<K,V> rightMap = broadcast.value();
        if (rightMap.containsKey(tuple._1)) {
            List<Tuple2<K, Tuple2<Optional<V>, W>>> list = Lists.newArrayList();
            for (V v: rightMap.get(tuple._1)) {
                list.add(new Tuple2<K, Tuple2<Optional<V>, W>>(tuple._1,
                        new Tuple2<Optional<V>, W>(Optional.of(v),tuple._2)));
            }
            return list;
        } else {
            return Collections.singletonList(new Tuple2<K, Tuple2<Optional<V>, W>>(tuple._1,
                    new Tuple2<Optional<V>, W>(Optional.<V>absent(),tuple._2)));
        }
    }
}