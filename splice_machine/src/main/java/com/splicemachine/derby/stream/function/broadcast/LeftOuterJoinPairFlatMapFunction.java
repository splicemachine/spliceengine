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
public class LeftOuterJoinPairFlatMapFunction<K,V,W> implements PairFlatMapFunction<Tuple2<K, V>, K, Tuple2<V, Optional<W>>>, Serializable {
    protected Broadcast<Multimap<K,W>> broadcast;
    public LeftOuterJoinPairFlatMapFunction() {}

    public LeftOuterJoinPairFlatMapFunction(Broadcast<Multimap<K,W>> broadcast) {
        this.broadcast = broadcast;
    }

    @Override
    public Iterable<Tuple2<K, Tuple2<V, Optional<W>>>> call(Tuple2<K, V> tuple) throws Exception {
        Multimap<K,W> rightMap = broadcast.value();
        if (rightMap.containsKey(tuple._1)) {
            List<Tuple2<K, Tuple2<V, Optional<W>>>> list = Lists.newArrayList();
            for (W w: rightMap.get(tuple._1)) {
                list.add(new Tuple2<K, Tuple2<V, Optional<W>>>(tuple._1,new Tuple2<V, Optional<W>>(tuple._2,Optional.of(w))));
            }
            return list;
        } else {
            return Collections.singletonList(new Tuple2<K, Tuple2<V, Optional<W>>>(tuple._1,
                    new Tuple2<V, Optional<W>>(tuple._2, Optional.<W>absent())));
        }
    }
}