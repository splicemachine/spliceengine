package com.splicemachine.stats.frequency;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Shorts;
import com.splicemachine.hash.Hash32;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/18/15
 */
public class ShortSpaceSaver extends ObjectSpaceSaver<Short> implements ShortFrequencyCounter{
    private static final Comparator<Short> comparator = new Comparator<Short>() {
        @Override
        public int compare(Short o1, Short o2) {
            return o1.compareTo(o2);
        }
    };
    private static final Function<? super FrequencyEstimate<Short>,ShortFrequencyEstimate> castFunction =
            new Function<FrequencyEstimate<Short>, ShortFrequencyEstimate>() {
                @Override public ShortFrequencyEstimate apply(FrequencyEstimate<Short> input) { return (ShortFrequencyEstimate)input; }
            };

    public ShortSpaceSaver(Hash32 hashFunction, int maxSize) {
        super(comparator, hashFunction, maxSize);
    }

    public ShortSpaceSaver( Hash32 hashFunction, int maxSize, int initialSize, float loadFactor) {
        super(comparator, hashFunction, maxSize, initialSize, loadFactor);
    }

    /*************************************************************************************************************/
    /*Accessors*/
    @Override
    public ShortFrequentElements heavyHitters(float support) {
        Collection<FrequencyEstimate<Short>> estimates = super.heavyItems(support);
        return new ShortFrequentElements(Collections2.transform(estimates, castFunction));
    }

    @Override
    public ShortFrequentElements frequentElements(int k) {
        Collection<FrequencyEstimate<Short>> estimates = super.topKElements(k);
        return new ShortFrequentElements(Collections2.transform(estimates,castFunction));
    }

    @Override
    public Set<? extends FrequencyEstimate<Short>> getFrequentElements(float support) {
        throw new UnsupportedOperationException("REMOVE");
    }

    /************************************************************************************************************/
    /*Modifiers*/
    @Override
    public void update(Short item) {
        assert item!=null: "Cannot estimate frequency of null elements!";
        update(item.shortValue(),1l);
    }

    @Override
    public void update(Short item, long count) {
        assert item!=null: "Cannot estimate frequency of null elements!";
        update(item.shortValue(),count);
    }

    @Override public void update(short item) { update(item,1l); }

    @Override
    public void update(short item, long count) {
        ((ShortEntry)holderEntry).value = item;
        doUpdate(count);
    }

    /*****************************************************************************************************************/
    /*Overridden methods*/
    @Override
    protected Entry newEntry() {
        return new ShortEntry();
    }

    @Override
    protected void setValue(Entry holderEntry, Entry entry) {
        ((ShortEntry)entry).value = ((ShortEntry)holderEntry).value;
    }

    private class ShortEntry extends Entry implements ShortFrequencyEstimate{
        short value;
        @Override public Short getValue() { return value; }
        @Override public short value() { return value; }

        @Override
        public void set(Short item) {
            this.value = item;
        }

        @Override
        public boolean equals(Entry o) {
            return ((ShortEntry)o).value==value;
        }

        @Override
        public int compareTo(ShortFrequencyEstimate o) {
            return Shorts.compare(value, o.value());
        }

        @Override
        protected int computeHash() {
            int hash = hashFunction.hash(value);
            if(hash==0)
                hash = 1;
            return hash;
        }
    }
}
