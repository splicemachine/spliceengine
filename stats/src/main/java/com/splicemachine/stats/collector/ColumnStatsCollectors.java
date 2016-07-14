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

package com.splicemachine.stats.collector;

import com.splicemachine.stats.cardinality.CardinalityEstimator;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.estimate.DistributionFactory;
import com.splicemachine.stats.frequency.FrequencyCounter;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.order.*;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class ColumnStatsCollectors {

    public static BooleanColumnStatsCollector booleanCollector(int columnId){
        return new BooleanColumn(columnId,FrequencyCounters.booleanCounter());
    }

    public static ByteColumnStatsCollector byteCollector(int columnId,int topK){
        return new ByteColumn(columnId,CardinalityEstimators.byteEstimator(),
                FrequencyCounters.byteCounter(),
                ByteMinMaxCollector.newInstance(),
                topK);
    }

    public static ShortColumnStatsCollector shortCollector(int columnId,short precision, short topK){
        return new ShortColumn(columnId,CardinalityEstimators.hyperLogLogShort(precision),
                FrequencyCounters.shortCounter((short)(2*topK)),
                ShortMinMaxCollector.newInstance(),
                topK);
    }

    public static IntColumnStatsCollector intCollector(int columnId,int precision, int topK){
        return new IntColumn(columnId,CardinalityEstimators.hyperLogLogInt(precision),
                FrequencyCounters.intCounter(2*topK),
                IntMinMaxCollector.newInstance(),
                topK);
    }

    public static LongColumnStatsCollector longCollector(int columnId,int precision, int topK){
        return new LongColumn(columnId,CardinalityEstimators.hyperLogLogLong(precision),
                FrequencyCounters.longCounter(2 * topK),
                LongMinMaxCollector.newInstance(),
                topK);
    }

    public static FloatColumnStatsCollector floatCollector(int columnId,int precision, int topK){
        return new FloatColumn(columnId,CardinalityEstimators.hyperLogLogFloat(precision),
                FrequencyCounters.floatCounter(2 * topK),
                FloatMinMaxCollector.newInstance(),
                topK);
    }

    public static DoubleColumnStatsCollector doubleCollector(int columnId,int precision, int topK){
        return new DoubleColumn(columnId,CardinalityEstimators.hyperLogLogDouble(precision),
                FrequencyCounters.doubleCounter(2 * topK),
                DoubleMinMaxCollector.newInstance(),
                topK);
    }


    public static <T extends Comparable<T>> ColumnStatsCollector<T> collector(int columnId,int precision, int topK,DistributionFactory<T> distributionFactory){
        CardinalityEstimator<T> estimator = CardinalityEstimators.hyperLogLog(precision);
        FrequencyCounter<T> counter = FrequencyCounters.counter(2*topK);
        ComparableMinMaxCollector<T> minMaxCollector = new ComparableMinMaxCollector<>();
        return new ComparableColumn<>(columnId,estimator, counter, minMaxCollector, topK,distributionFactory);
    }
}
