/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
