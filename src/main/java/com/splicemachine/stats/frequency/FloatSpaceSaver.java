package com.splicemachine.stats.frequency;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Floats;
import com.splicemachine.hash.Hash32;

import java.util.Collection;
import java.util.Comparator;

/**
 * @author Scott Fines
 *         Date: 2/18/15
 */
public class FloatSpaceSaver extends ObjectSpaceSaver<Float> implements FloatFrequencyCounter{
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
        return new FloatFrequentElements(Collections2.transform(estimates, castFunction));
    }

    @Override
    public FloatFrequentElements frequentElements(int k) {
        Collection<FrequencyEstimate<Float>> estimates = super.topKElements(k);
        return new FloatFrequentElements(Collections2.transform(estimates,castFunction));
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
        ((FloatEntry)holderEntry).value = item;
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

        @Override
        protected int computeHash() {
            int hash = hashFunction.hash(Float.floatToRawIntBits(value));
            if(hash==0)
                hash = 1;
            return hash;
        }
    }
}
