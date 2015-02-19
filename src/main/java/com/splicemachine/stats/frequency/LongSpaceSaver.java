package com.splicemachine.stats.frequency;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Longs;
import com.splicemachine.hash.Hash32;

import java.util.Collection;
import java.util.Comparator;

/**
 * @author Scott Fines
 *         Date: 1/30/15
 */
public class LongSpaceSaver extends ObjectSpaceSaver<Long> implements LongFrequencyCounter {
    private static final Comparator<Long> comparator = new Comparator<Long>() {
        @Override public int compare(Long o1, Long o2) { return o1.compareTo(o2); }
    };
    private static final Function<? super FrequencyEstimate<Long>,LongFrequencyEstimate> castFunction =
            new Function<FrequencyEstimate<Long>, LongFrequencyEstimate>() {
        @Override public LongFrequencyEstimate apply(FrequencyEstimate<Long> input) { return (LongFrequencyEstimate)input; }
    };

    public LongSpaceSaver(Hash32 hashFunction, int maxSize) {
        super(comparator, hashFunction, maxSize);
    }

    public LongSpaceSaver( Hash32 hashFunction, int maxSize, int initialSize, float loadFactor) {
        super(comparator, hashFunction, maxSize, initialSize, loadFactor);
    }

    /*************************************************************************************************************/
    /*Accessors*/
    @Override
    public LongFrequentElements heavyHitters(float support) {
        Collection<FrequencyEstimate<Long>> estimates = super.heavyItems(support);
        return new LongFrequentElements(Collections2.transform(estimates,castFunction));
    }

    @Override
    public LongFrequentElements frequentElements(int k) {
        Collection<FrequencyEstimate<Long>> estimates = super.topKElements(k);
        return new LongFrequentElements(Collections2.transform(estimates,castFunction));
    }

    /************************************************************************************************************/
    /*Modifiers*/
    @Override
    public void update(Long item) {
        assert item!=null: "Cannot estimate frequency of null elements!";
        update(item.longValue(),1l);
    }

    @Override
    public void update(Long item, long count) {
        assert item!=null: "Cannot estimate frequency of null elements!";
        update(item.longValue(),count);
    }

    @Override public void update(long item) { update(item,1l); }

    @Override
    public void update(long item, long count) {
        ((LongEntry)holderEntry).value = item;
        doUpdate(count);
    }

    /*****************************************************************************************************************/
    /*Overridden methods*/
    @Override
    protected Entry newEntry() {
        return new LongEntry();
    }

    @Override
    protected void setValue(Entry holderEntry, Entry entry) {
        ((LongEntry)entry).value = ((LongEntry)holderEntry).value;
    }

    private class LongEntry extends Entry implements LongFrequencyEstimate{
        long value;
        @Override public Long getValue() { return value; }
        @Override public long value() { return value; }

        @Override
        public void set(Long item) {
            this.value = item;
        }

        @Override
        public boolean equals(Entry o) {
            return ((LongEntry)o).value==value;
        }

        @Override
        public int compareTo(LongFrequencyEstimate o) {
            return Longs.compare(value,o.value());
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
