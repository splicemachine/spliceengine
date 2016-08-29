/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.control;

import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.*;
import scala.Tuple2;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;


/**
 * Created by dgomezferro on 7/31/15.
 */
public class ControlUtils {
    public static <K, V> Iterator<Tuple2<K, V>> entryToTuple(Collection<Map.Entry<K, V>> collection) {
        return Iterators.transform(collection.iterator(),new Function<Map.Entry<K, V>, Tuple2<K, V>>() {

            @Nullable
            @Override
            public Tuple2<K, V> apply(@Nullable Map.Entry<K, V> kvEntry) {
                assert kvEntry!=null;
                return new Tuple2<>(kvEntry.getKey(),kvEntry.getValue());
            }
        });
    }

    public static <K, V> Multimap<K,V> multimapFromIterator(Iterator<Tuple2<K, V>> iterator) {
        Multimap<K,V> newMap = ArrayListMultimap.create();
        while (iterator.hasNext()) {
            Tuple2<K, V> t = iterator.next();
            newMap.put(t._1(), t._2());
        }
        return newMap;
    }
}
