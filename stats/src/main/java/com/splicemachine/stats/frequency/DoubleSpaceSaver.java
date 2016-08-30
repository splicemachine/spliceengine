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

import com.google.common.base.Function;
import org.sparkproject.guava.collect.Collections2;
import org.sparkproject.guava.primitives.Doubles;
import com.splicemachine.hash.Hash32;

import java.util.Collection;
import java.util.Comparator;

/**
 * @author Scott Fines
 *         Date: 2/18/15
 */
class DoubleSpaceSaver extends ObjectSpaceSaver<Double> implements DoubleFrequencyCounter {
    private static final Comparator<Double> comparator = new Comparator<Double>() {
        @Override
        public int compare(Double o1, Double o2) {
            return o1.compareTo(o2);
        }
    };
    private static final Function<? super FrequencyEstimate<Double>,DoubleFrequencyEstimate> castFunction =
            new Function<FrequencyEstimate<Double>, DoubleFrequencyEstimate>() {
                @Override public DoubleFrequencyEstimate apply(FrequencyEstimate<Double> input) { return (DoubleFrequencyEstimate)input; }
            };

    public DoubleSpaceSaver(Hash32 hashFunction, int maxSize) {
        super(comparator, hashFunction, maxSize);
    }

    public DoubleSpaceSaver( Hash32 hashFunction, int maxSize, int initialSize, float loadFactor) {
        super(comparator, hashFunction, maxSize, initialSize, loadFactor);
    }

    /*************************************************************************************************************/
    /*Accessors*/
    @Override
    public DoubleFrequentElements heavyHitters(float support) {
        Collection<FrequencyEstimate<Double>> estimates = super.heavyItems(support);
        return DoubleFrequentElements.heavyHitters(support,totalCount(),Collections2.transform(estimates, castFunction));
    }

    @Override
    public DoubleFrequentElements frequentElements(int k) {
        Collection<FrequencyEstimate<Double>> estimates = super.topKElements(k);
        return DoubleFrequentElements.topK(k,totalCount(),Collections2.transform(estimates,castFunction));
    }

    /************************************************************************************************************/
    /*Modifiers*/
    @Override
    public void update(Double item) {
        assert item!=null: "Cannot estimate frequency of null elements!";
        update(item.longValue(),1l);
    }

    @Override
    public void update(Double item, long count) {
        assert item!=null: "Cannot estimate frequency of null elements!";
        update(item.longValue(),count);
    }

    @Override public void update(double item) { update(item,1l); }

    @Override
    public void update(double item, long count) {
        ((DoubleEntry)holderEntry).setValue(item);
        doUpdate(count);
    }

    /*****************************************************************************************************************/
    /*Overridden methods*/
    @Override
    protected Entry newEntry() {
        return new DoubleEntry();
    }

    @Override
    protected void setValue(Entry holderEntry, Entry entry) {
        ((DoubleEntry)entry).value = ((DoubleEntry)holderEntry).value;
    }

    private class DoubleEntry extends Entry implements DoubleFrequencyEstimate{
        double value;
        @Override public Double getValue() { return value; }
        @Override public double value() { return value; }

        @Override
        public void set(Double item) {
            this.value = item;
        }

        @Override
        public boolean equals(Entry o) {
            return ((DoubleEntry)o).value==value;
        }

        @Override
        public int compareTo(DoubleFrequencyEstimate o) {
            return Doubles.compare(value, o.value());
        }

        public void setValue(double item){
            this.value = item;
            this.hashCode = 0;
        }

        @Override
        protected int computeHash() {
            int hash = hashFunction.hash(Double.doubleToRawLongBits(value));
            if(hash==0)
                hash = 1;
            return hash;
        }
    }
}
