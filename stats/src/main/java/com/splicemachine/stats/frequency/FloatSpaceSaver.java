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

import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.Collections2;
import org.spark_project.guava.primitives.Floats;
import com.splicemachine.hash.Hash32;
import java.util.Collection;
import java.util.Comparator;

/**
 * @author Scott Fines
 *         Date: 2/18/15
 */
class FloatSpaceSaver extends ObjectSpaceSaver<Float> implements FloatFrequencyCounter{
    private static final Comparator<Float> comparator = new Comparator<Float>() {
        @Override
        public int compare(Float o1, Float o2) {
            return o1.compareTo(o2);
        }
    };
    private static final Function<? super FrequencyEstimate<Float>,FloatFrequencyEstimate> castFunction =
            new Function<FrequencyEstimate<Float>, FloatFrequencyEstimate>() {
                @Override public FloatFrequencyEstimate apply(FrequencyEstimate<Float> input) { return (FloatFrequencyEstimate)input; }
            };

    public FloatSpaceSaver(Hash32 hashFunction, int maxSize) {
        super(comparator, hashFunction, maxSize);
    }

    public FloatSpaceSaver( Hash32 hashFunction, int maxSize, int initialSize, float loadFactor) {
        super(comparator, hashFunction, maxSize, initialSize, loadFactor);
    }

    /*************************************************************************************************************/
    /*Accessors*/
    @Override
    public FloatFrequentElements heavyHitters(float support) {
        Collection<FrequencyEstimate<Float>> estimates = super.heavyItems(support);
        return FloatFrequentElements.heavyHitters(support,totalCount(),Collections2.transform(estimates, castFunction));
    }

    @Override
    public FloatFrequentElements frequentElements(int k) {
        Collection<FrequencyEstimate<Float>> estimates = super.topKElements(k);
        return FloatFrequentElements.topK(k,totalCount(),Collections2.transform(estimates,castFunction));
    }

    /************************************************************************************************************/
    /*Modifiers*/
    @Override
    public void update(Float item) {
        assert item!=null: "Cannot estimate frequency of null elements!";
        update(item.longValue(),1l);
    }

    @Override
    public void update(Float item, long count) {
        assert item!=null: "Cannot estimate frequency of null elements!";
        update(item.longValue(),count);
    }

    @Override public void update(float item) { update(item,1l); }

    @Override
    public void update(float item, long count) {
        ((FloatEntry)holderEntry).setValue(item);
        doUpdate(count);
    }

    /*****************************************************************************************************************/
    /*Overridden methods*/
    @Override
    protected Entry newEntry() {
        return new FloatEntry();
    }

    @Override
    protected void setValue(Entry holderEntry, Entry entry) {
        ((FloatEntry)entry).value = ((FloatEntry)holderEntry).value;
    }

    private class FloatEntry extends Entry implements FloatFrequencyEstimate{
        float value;
        @Override public Float getValue() { return value; }
        @Override public float value() { return value; }

        @Override
        public void set(Float item) {
            this.value = item;
        }

        @Override
        public boolean equals(Entry o) {
            return ((FloatEntry)o).value==value;
        }

        @Override
        public int compareTo(FloatFrequencyEstimate o) {
            return Floats.compare(value, o.value());
        }

        public void setValue(float item){
            this.value = item;
            this.hashCode = 0;
        }

        @Override
        protected int computeHash() {
            int hash = hashFunction.hash(Float.floatToRawIntBits(value));
            if(hash==0)
                hash = 1;
            return hash;
        }
    }
}
