package com.splicemachine.stats.frequency;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Ints;
import com.splicemachine.hash.Hash32;

import java.util.Collection;
import java.util.Comparator;

/**
 * @author Scott Fines
 *         Date: 2/18/15
 */
class IntSpaceSaver extends ObjectSpaceSaver<Integer> implements IntFrequencyCounter {
    private static final Comparator<Integer> comparator = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
    };
    private static final Function<? super FrequencyEstimate<Integer>,IntFrequencyEstimate> castFunction =
            new Function<FrequencyEstimate<Integer>, IntFrequencyEstimate>() {
                @Override public IntFrequencyEstimate apply(FrequencyEstimate<Integer> input) { return (IntFrequencyEstimate)input; }
            };

    public IntSpaceSaver(Hash32 hashFunction, int maxSize) {
        super(comparator, hashFunction, maxSize);
    }

    public IntSpaceSaver( Hash32 hashFunction, int maxSize, int initialSize, float loadFactor) {
        super(comparator, hashFunction, maxSize, initialSize, loadFactor);
    }

    /*************************************************************************************************************/
    /*Accessors*/
    @Override
    public IntFrequentElements heavyHitters(float support) {
        Collection<FrequencyEstimate<Integer>> estimates = super.heavyItems(support);
        return IntFrequentElements.heavyHitters(support,totalCount(),Collections2.transform(estimates, castFunction));
    }

    @Override
    public IntFrequentElements frequentElements(int k) {
        Collection<FrequencyEstimate<Integer>> estimates = super.topKElements(k);
        return IntFrequentElements.topK(k,totalCount(),Collections2.transform(estimates,castFunction));
    }

    /************************************************************************************************************/
    /*Modifiers*/
    @Override
    public void update(Integer item) {
        assert item!=null: "Cannot estimate frequency of null elements!";
        update(item.intValue(),1l);
    }

    @Override
    public void update(Integer item, long count) {
        assert item!=null: "Cannot estimate frequency of null elements!";
        update(item.intValue(),count);
    }

    @Override public void update(int item) { update(item,1l); }

    @Override
    public void update(int item, long count) {
        ((IntegerEntry)holderEntry).value = item;
        doUpdate(count);
    }

    /*****************************************************************************************************************/
    /*Overridden methods*/
    @Override
    protected Entry newEntry() {
        return new IntegerEntry();
    }

    @Override
    protected void setValue(ObjectSpaceSaver.Entry holderEntry, ObjectSpaceSaver.Entry entry) {
        ((IntegerEntry)entry).value = ((IntegerEntry)holderEntry).value;
    }

    private class IntegerEntry extends Entry implements IntFrequencyEstimate{
        int value;
        @Override public Integer getValue() { return value; }
        @Override public int value() { return value; }

        @Override
        public void set(Integer item) {
            this.value = item;
        }

        @Override
        public boolean equals(Entry o) {
            return ((IntegerEntry)o).value==value;
        }

        @Override
        public int compareTo(IntFrequencyEstimate o) {
            return Ints.compare(value, o.value());
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
