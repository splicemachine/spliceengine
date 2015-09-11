package com.splicemachine.derby.stream.function.broadcast;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import scala.Tuple2;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Created by jleach on 5/27/15.
 */
public class JoinPairFlatMapFunction<K,V,W> implements PairFlatMapFunction<Tuple2<K, V>, K, Tuple2<V, W>>, Serializable {
    protected Broadcast<List<Tuple2<K, W>>> broadcast;
    protected Multimap<K, W> rightMap;
    public JoinPairFlatMapFunction() {}

    public JoinPairFlatMapFunction(Broadcast<List<Tuple2<K, W>>> broadcast) {
        this.broadcast = broadcast;
    }
    private Multimap<K, W> collectAsMap(List<Tuple2<K, W>> collected) {
        Multimap<K, W> result = ArrayListMultimap.create();
        for (Tuple2<K, W> e : collected) {
                result.put(e._1(), e._2());
            }
        return result;
    }


    @Override
    public Iterable<Tuple2<K, Tuple2<V, W>>> call(Tuple2<K, V> tuple) throws Exception {
        if (rightMap == null) {
            rightMap = collectAsMap(broadcast.value());
        }
        if (rightMap.containsKey(tuple._1)) {
            List<Tuple2<K, Tuple2<V, W>>> list = Lists.newArrayList();
            for (W w: rightMap.get(tuple._1)) {
                list.add(new Tuple2<K, Tuple2<V, W>>(tuple._1,new Tuple2<V, W>(tuple._2,w)));
            }
            return list;
        } else {
            return Collections.EMPTY_LIST;
        }
    }
}