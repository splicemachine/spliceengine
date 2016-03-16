package com.splicemachine.derby.stream.control;

import com.google.common.base.Function;
import org.sparkproject.guava.collect.ArrayListMultimap;
import org.sparkproject.guava.collect.FluentIterable;
import org.sparkproject.guava.collect.Multimap;
import scala.Tuple2;
import javax.annotation.Nullable;
import java.util.Map;


/**
 * Created by dgomezferro on 7/31/15.
 */
public class ControlUtils {
    public static <K, V> Iterable<Tuple2<K, V>> entryToTuple(Iterable<Map.Entry<K, V>> iterable) {
        return FluentIterable.from(iterable).transform(new Function<Map.Entry<K, V>, Tuple2<K, V>>() {

            @Nullable
            @Override
            public Tuple2<K, V> apply(@Nullable Map.Entry<K, V> kvEntry) {
                assert kvEntry!=null;
                return new Tuple2<>(kvEntry.getKey(),kvEntry.getValue());
            }
        });
    }

    public static <K, V> Multimap<K,V> multimapFromIterable(Iterable<Tuple2<K, V>> iterable) {
        Multimap<K,V> newMap = ArrayListMultimap.create();
        for (Tuple2<K, V> t : iterable) {
            newMap.put(t._1(), t._2());
        }
        return newMap;
    }
}
