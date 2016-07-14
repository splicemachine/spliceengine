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
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.stats.cardinality.BytesCardinalityEstimator;
import com.splicemachine.stats.cardinality.CardinalityEstimator;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.frequency.BytesFrequentElements;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class BytesColumnStatistics extends BaseColumnStatistics<ByteBuffer> {
    private BytesCardinalityEstimator cardinalityEstimator;
    private BytesFrequentElements frequentElements;
    private byte[] min;
    private byte[] max;

    private ByteComparator byteComparator;

    public BytesColumnStatistics(int columnId,
                                 BytesCardinalityEstimator cardinalityEstimator,
                                 BytesFrequentElements frequentElements,
                                 ByteComparator byteComparator,
                                 byte[] min,
                                 byte[] max,
                                 long totalBytes,
                                 long totalCount,
                                 long nullCount,
                                 long minCount) {
       super(columnId, totalBytes, totalCount, nullCount,minCount);
        this.cardinalityEstimator = cardinalityEstimator;
        this.byteComparator = byteComparator;
        this.frequentElements = frequentElements;
        this.min = min;
        this.max = max;
    }

    @Override
    public Distribution<ByteBuffer> getDistribution() {
        throw new UnsupportedOperationException("IMPLEMENT!");
    }

    @Override public long cardinality() { return cardinalityEstimator.getEstimate(); }
    @Override public FrequentElements<ByteBuffer> topK() { return frequentElements; }
    @Override public ByteBuffer minValue() { return ByteBuffer.wrap(min); }
    @Override public ByteBuffer maxValue() { return ByteBuffer.wrap(max); }
    public byte[] min(){ return min; }
    public byte[] max(){ return max; }

    @Override
    public CardinalityEstimator getCardinalityEstimator() {
        return cardinalityEstimator;
    }

    @Override
    public ColumnStatistics<ByteBuffer> getClone() {
        return new BytesColumnStatistics(columnId,cardinalityEstimator.getClone(),
                frequentElements.getClone(),
                byteComparator,
                min,
                max,
                totalBytes,
                totalCount,
                nullCount,
                minCount);
    }

    @Override
    public ColumnStatistics<ByteBuffer> merge(ColumnStatistics<ByteBuffer> other) {
        assert other.getCardinalityEstimator() instanceof BytesCardinalityEstimator: "Cannot merge statistics of type "+ other.getClass();
        cardinalityEstimator = cardinalityEstimator.merge((BytesCardinalityEstimator)other.getCardinalityEstimator());
        frequentElements = frequentElements.merge((BytesFrequentElements)other.topK());
        if(byteComparator.compare(other.minValue().array(),min)>0)
            min = other.minValue().array();
        if(byteComparator.compare(other.maxValue().array(), max)>0)
            max = other.maxValue().array();
        totalBytes+=other.totalBytes();
        totalCount+=other.nullCount()+other.nonNullCount();
        nullCount+=other.nullCount();
        return this;
    }

    public Encoder<BytesColumnStatistics> encoder(){
        return new EncDec(byteComparator);
    }

    static class EncDec implements Encoder<BytesColumnStatistics> {
        private final ByteComparator byteComparator;

        public EncDec(ByteComparator byteComparator) {
            this.byteComparator = byteComparator;
        }

        @Override
        public void encode(BytesColumnStatistics item,DataOutput encoder) throws IOException {
            BaseColumnStatistics.write(item,encoder);
            encoder.writeInt(item.min.length);
            encoder.write(item.min);
            encoder.writeInt(item.max.length);
            encoder.write(item.max);
            CardinalityEstimators.bytesEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.byteArrayEncoder().encode(item.frequentElements, encoder);
        }

        @Override
        public BytesColumnStatistics decode(DataInput decoder) throws IOException {
            int columnId = decoder.readInt();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            long minCount = decoder.readLong();
            byte[] min = new byte[decoder.readInt()];
            decoder.readFully(min);
            byte[] max = new byte[decoder.readInt()];
            decoder.readFully(max);
            BytesCardinalityEstimator cardinalityEstimator = CardinalityEstimators.bytesEncoder().decode(decoder);
            BytesFrequentElements frequentElements = FrequencyCounters.byteArrayEncoder().decode(decoder);
            return new BytesColumnStatistics(columnId,cardinalityEstimator, frequentElements,
                    byteComparator,min,max,totalBytes,totalCount,nullCount,minCount);
        }
    }
}
