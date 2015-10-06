package com.splicemachine.derby.stream.function.broadcast;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import scala.Tuple2;
import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Created by jleach on 5/27/15.
 */
public class CoGroupPairFunction<K,V,W> implements PairFunction<Tuple2<K, Iterable<V>>, K, Tuple2<Iterable<V>, Iterable<W>>>, Serializable {
    protected Broadcast<Multimap<K,W>> broadcast;
    public CoGroupPairFunction() {}

    public CoGroupPairFunction(Broadcast<Multimap<K, W>> broadcast) {
        this.broadcast = broadcast;
    }

    @Override
    public Tuple2<K, Tuple2<Iterable<V>, Iterable<W>>> call(Tuple2<K, Iterable<V>> tuple) throws Exception {
        Multimap<K,W> map = broadcast.value();
        return new Tuple2(tuple._1,new Tuple2(tuple._2,map.get(tuple._1)));
    }

}