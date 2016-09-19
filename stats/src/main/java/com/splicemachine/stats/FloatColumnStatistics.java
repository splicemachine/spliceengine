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

package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.CardinalityEstimator;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.cardinality.FloatCardinalityEstimator;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.UniformFloatDistribution;
import com.splicemachine.stats.frequency.FloatFrequentElements;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public class FloatColumnStatistics extends BaseColumnStatistics<Float> {
    private FloatCardinalityEstimator cardinalityEstimator;
    private FloatFrequentElements frequentElements;
    private float min;
    private float max;

    private transient Distribution<Float> distribution;

    public FloatColumnStatistics(int columnId,
                                 FloatCardinalityEstimator cardinalityEstimator,
                                 FloatFrequentElements frequentElements,
                                 float min,
                                 float max,
                                 long totalBytes,
                                 long totalCount,
                                 long nullCount,
                                 long minCount) {
        super(columnId,totalBytes,totalCount,nullCount,minCount);
        this.cardinalityEstimator = cardinalityEstimator;
        this.frequentElements = frequentElements;
        this.min = min;
        this.max = max;
        this.distribution = new UniformFloatDistribution(this);
    }

    @Override public long cardinality() { return cardinalityEstimator.getEstimate(); }
    @Override public FrequentElements<Float> topK() { return frequentElements; }
    @Override public Float minValue() { return min; }
    @Override public Float maxValue() { return max; }
    public float min(){ return min; }
    public float max(){ return max; }

    @Override
    public Distribution<Float> getDistribution() {
        return distribution;
    }

    @Override
    public ColumnStatistics<Float> getClone() {
        return new FloatColumnStatistics(columnId,cardinalityEstimator.newCopy(),
                frequentElements.newCopy(),
                min,
                max,
                totalBytes,
                totalCount,
                nullCount,
                minCount);
    }

    @Override
    public CardinalityEstimator getCardinalityEstimator() {
        return cardinalityEstimator;
    }

    @Override
    public ColumnStatistics<Float> merge(ColumnStatistics<Float> other) {
        assert other.getCardinalityEstimator() instanceof FloatCardinalityEstimator: "Cannot merge statistics of type "+ other.getClass();
        cardinalityEstimator = (FloatCardinalityEstimator)cardinalityEstimator.merge(other.getCardinalityEstimator());
        frequentElements = (FloatFrequentElements)frequentElements.merge(other.topK());
        if(other.minValue()<min)
            min = other.minValue();
        if(other.maxValue()>max)
            max = other.maxValue();
        totalBytes+=other.totalBytes();
        totalCount+=other.nonNullCount()+other.nullCount();
        nullCount+=other.nullCount();
        return this;
    }

    public static Encoder<FloatColumnStatistics> encoder(){
        return EncDec.INSTANCE;
    }

    static class EncDec implements Encoder<FloatColumnStatistics> {
        public static final EncDec INSTANCE = new EncDec();

        @Override
        public void encode(FloatColumnStatistics item,DataOutput encoder) throws IOException {
            BaseColumnStatistics.write(item, encoder);
            encoder.writeFloat(item.min);
            encoder.writeFloat(item.max);
            CardinalityEstimators.floatEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.floatEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public FloatColumnStatistics decode(DataInput decoder) throws IOException {
            int columnId = decoder.readInt();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            long minCount = decoder.readLong();
            float min = decoder.readFloat();
            float max = decoder.readFloat();
            FloatCardinalityEstimator cardinalityEstimator = CardinalityEstimators.floatEncoder().decode(decoder);
            FloatFrequentElements frequentElements = FrequencyCounters.floatEncoder().decode(decoder);
            return new FloatColumnStatistics(columnId,cardinalityEstimator,frequentElements,min,max,totalBytes,totalCount,nullCount,minCount);
        }
    }
}
