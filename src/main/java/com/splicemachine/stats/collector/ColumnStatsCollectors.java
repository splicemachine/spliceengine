package com.splicemachine.stats.collector;

import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.stats.cardinality.CardinalityEstimator;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.frequency.FrequencyCounter;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.order.*;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class ColumnStatsCollectors {

    public static BooleanColumnStatsCollector booleanCollector(){
        return new BooleanColumn(FrequencyCounters.booleanCounter());
    }

    public static ByteColumnStatsCollector byteCollector(int topK){
        return new ByteColumn(CardinalityEstimators.byteEstimator(),
                FrequencyCounters.byteCounter(),
                ByteMinMaxCollector.newInstance(),
                topK);
    }

    public static ShortColumnStatsCollector shortCollector(short precision, short topK){
        return new ShortColumn(CardinalityEstimators.hyperLogLogShort(precision),
                FrequencyCounters.shortCounter((short)(2*topK)),
                ShortMinMaxCollector.newInstance(),
                topK);
    }

    public static IntColumnStatsCollector intCollector(int precision, int topK){
        return new IntColumn(CardinalityEstimators.hyperLogLogInt(precision),
                FrequencyCounters.intCounter(2*topK),
                IntMinMaxCollector.newInstance(),
                topK);
    }

    public static LongColumnStatsCollector longCollector(int precision, int topK){
        return new LongColumn(CardinalityEstimators.hyperLogLogLong(precision),
                FrequencyCounters.longCounter(2 * topK),
                LongMinMaxCollector.newInstance(),
                topK);
    }

    public static FloatColumnStatsCollector floatCollector(int precision, int topK){
        return new FloatColumn(CardinalityEstimators.hyperLogLogFloat(precision),
                FrequencyCounters.floatCounter(2 * topK),
                FloatMinMaxCollector.newInstance(),
                topK);
    }

    public static DoubleColumnStatsCollector doubleCollector(int precision, int topK){
        return new DoubleColumn(CardinalityEstimators.hyperLogLogDouble(precision),
                FrequencyCounters.doubleCounter(2 * topK),
                DoubleMinMaxCollector.newInstance(),
                topK);
    }

    public static BytesColumnStatsCollector bytesCollector(int precision, int topK){
        ByteComparator byteComparator = Bytes.basicByteComparator();
        return bytesCollector(precision, topK, byteComparator);
    }

    private static BytesColumnStatsCollector bytesCollector(int precision, int topK, ByteComparator byteComparator) {
        return new BytesColumn(CardinalityEstimators.hyperLogLogBytes(precision),
                FrequencyCounters.byteArrayCounter(2 * topK),
                new BufferMinMaxCollector(byteComparator),
                byteComparator,
                topK);
    }

    public static <T extends Comparable<T>> ColumnStatsCollector<T> collector(int precision, int topK){
        CardinalityEstimator<T> estimator = CardinalityEstimators.hyperLogLog(precision);
        FrequencyCounter<T> counter = FrequencyCounters.counter(2*topK);
        ComparableMinMaxCollector<T> minMaxCollector = new ComparableMinMaxCollector<>();
        return new ComparableColumn<>(estimator, counter, minMaxCollector, topK);
    }

}
