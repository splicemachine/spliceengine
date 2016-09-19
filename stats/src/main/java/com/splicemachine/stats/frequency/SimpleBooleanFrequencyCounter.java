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

package com.splicemachine.stats.frequency;

import org.spark_project.guava.collect.Iterators;
import com.splicemachine.encoding.Encoder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 * Date: 3/26/14
 */
class SimpleBooleanFrequencyCounter implements BooleanFrequencyCounter {
		private Frequency trueFrequency = new Frequency(true);
		private Frequency falseFrequency = new Frequency(false);

		@Override
		public void update(boolean item) {
				if(item) trueFrequency.count++;
				else falseFrequency.count++;
		}

		@Override
		public void update(Boolean item) {
				if(item==Boolean.TRUE) trueFrequency.count++;
				else falseFrequency.count++;
		}

		@Override
		public void update(Boolean item, long count) {
				if(item==Boolean.TRUE) trueFrequency.count+=count;
				else falseFrequency.count+=count;
		}

    @Override
		public void update(boolean item, long count) {
				if(item) trueFrequency.count+=count;
				else falseFrequency.count+=count;
		}

		@Override
		public BooleanFrequentElements frequencies() {
				return new SimpleBooleanFrequentElements(trueFrequency.count,falseFrequency.count);
		}

		@Override
		public FrequentElements<Boolean> frequentElements(int k) {
				if(k==0) return new SimpleBooleanFrequentElements(0l,0l);
				if(k==1) return new SimpleBooleanFrequentElements(trueFrequency.count,0l);
				return frequencies();
		}

    @Override
    public FrequentElements<Boolean> heavyHitters(float support) {
        long total = trueFrequency.count+falseFrequency.count;
        long supportLevel = (long)support*total;
        if(trueFrequency.count>=supportLevel){
            if(falseFrequency.count>=supportLevel) return frequencies();
            else return new SimpleBooleanFrequentElements(trueFrequency.count,0l);
        }else if(falseFrequency.count>=supportLevel){
            return new SimpleBooleanFrequentElements(falseFrequency.count,0l);
        }
        return new SimpleBooleanFrequentElements(0l,0l);
    }

		private static class Frequency implements BooleanFrequencyEstimate{
				private long count;
				private final boolean value;

				private Frequency(boolean value) {
						this.value = value;
				}

				@Override public boolean value() { return value; }
				@Override public Boolean getValue() { return value; }
				@Override public long count() { return count; }
				@Override public long error() { return 0; } //no error! whoo!

        @Override
        public FrequencyEstimate<Boolean> merge(FrequencyEstimate<Boolean> other) {
            this.count+=other.count();
            return this;
        }
    }

    static class EncoderDecoder implements Encoder<BooleanFrequentElements> {
        public static final EncoderDecoder INSTANCE = new EncoderDecoder();

        @Override
        public void encode(BooleanFrequentElements item, DataOutput dataInput) throws IOException {
            BooleanFrequencyEstimate trueEst = item.equalsTrue();
            dataInput.writeLong(trueEst.count());
            BooleanFrequencyEstimate falseEst = item.equalsFalse();
            dataInput.writeLong(falseEst.count());
        }

        @Override
        public BooleanFrequentElements decode(DataInput input) throws IOException {
            long trueCount = input.readLong();
            long falseCount = input.readLong();
            return new SimpleBooleanFrequentElements(trueCount,falseCount);
        }
    }

}
