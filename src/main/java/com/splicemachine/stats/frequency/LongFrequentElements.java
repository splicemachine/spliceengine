package com.splicemachine.stats.frequency;

import com.google.common.primitives.Longs;
import com.splicemachine.encoding.Encoder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Long-specific representation of FrequentElements for a long.
 *
 * This allows primitive-specific queries against long values, without the need for autoboxing.
 *
 * @author Scott Fines
 *         Date: 12/5/14
 */
public abstract class LongFrequentElements implements FrequentElements<Long> {
    private static final Comparator<LongFrequencyEstimate> naturalComparator = new Comparator<LongFrequencyEstimate>() {
        @Override
        public int compare(LongFrequencyEstimate o1, LongFrequencyEstimate o2) {
            int compare = Longs.compare(o1.value(),o2.value());
            if(compare!=0) return compare;
            return Long.signum(o1.count()-o2.count()); //should never happen, but just in case
        }
    };

    private NavigableSet<LongFrequencyEstimate> elements;
    private long totalCount;

    private LongFrequentElements(long totalCount,Collection<LongFrequencyEstimate> elements) {
        TreeSet<LongFrequencyEstimate> elems = new TreeSet<>(naturalComparator);
        elems.addAll(elements);
        this.elements = elems;
        this.totalCount = totalCount;
    }

    public static LongFrequentElements topK(int k, long totalCount,Collection<LongFrequencyEstimate> elements){
        return new TopK(k,totalCount,elements);
    }

    public static LongFrequentElements heavyHitters(float support, long totalCount,Collection<LongFrequencyEstimate> elements){
        return new HeavyItems(support,totalCount,elements);
    }

    @Override
    public Set<? extends FrequencyEstimate<Long>> allFrequentElements() {
        return Collections.unmodifiableSet(elements);
    }

    public LongFrequencyEstimate countEqual(long item){
        for(LongFrequencyEstimate est:elements){
            if(est.value()==item) return est;
        }
        return new ZeroFreq(item);
    }

    public Set<LongFrequencyEstimate> frequentBetween(long start, long stop, boolean includeStart, boolean includeStop){
        LongFrequencyEstimate first = elements.first();
        if(first.value()>stop) return Collections.emptySet();
        else if(first.value()==stop){
            if(includeStop) return Collections.singleton(first);
            else return Collections.emptySet();
        }else if(first.value()>start || (includeStart && first.value()==start))
            return frequentBefore(stop,includeStop);

        LongFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }else if(last.value()<stop || (includeStop && last.value()==stop)) return frequentAfter(start,includeStart);

        ZeroFreq est = new ZeroFreq(start);
        LongFrequencyEstimate rangeStart = elements.ceiling(est);
        est.item = stop;
        LongFrequencyEstimate rangeStop = elements.ceiling(est);
        return Collections.unmodifiableSet(elements.subSet(rangeStart,includeStart,rangeStop,includeStop));
    }

    public Set<LongFrequencyEstimate> frequentAfter(long start, boolean includeStart){
        LongFrequencyEstimate first = elements.first();
        if(first.value()>start) return elements;
        else if(first.value()==start){
            if(includeStart) return elements;
            else return elements.tailSet(first, false);
        }
        LongFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }

        //find the starting value
        LongFrequencyEstimate rangeStart = elements.ceiling(new ZeroFreq(start));
        return Collections.unmodifiableSet(elements.tailSet(rangeStart, includeStart));
    }

    public Set<LongFrequencyEstimate> frequentBefore(long stop, boolean includeStop){
        LongFrequencyEstimate first = elements.first();
        if(first.value()>stop)return Collections.emptySet();
        else if(first.value()==stop){
            if(!includeStop) return Collections.emptySet();
            else return Collections.singleton(first);
        }
        LongFrequencyEstimate last = elements.last();
        if(last.value()<stop) return elements;
        else if(last.value()==stop){
            if(includeStop) return elements;
            else return elements.headSet(last);
        }

        LongFrequencyEstimate rangeStop = elements.higher(new ZeroFreq(stop));
        return Collections.unmodifiableSet(elements.headSet(rangeStop, includeStop));
    }

    @Override
    public FrequencyEstimate<? extends Long> equal(Long item) {
        assert item!=null: "Cannot estimate null matches!";
        return countEqual(item);
    }

    @Override
    public Set<? extends FrequencyEstimate<Long>> frequentElementsBetween(Long start, Long stop, boolean includeMin, boolean includeStop) {
        if(start==null){
            if(stop==null){
                return elements;
            }else
                return frequentBefore(stop,includeStop);
        }else if(stop==null){
            return frequentAfter(start,includeMin);
        }else{
            if(start.equals(stop)){
                if(includeMin||includeStop) return Collections.singleton(countEqual(start));
                else return Collections.emptySet();
            }else
                return frequentBetween(start,stop,includeMin,includeStop);
        }
    }

    @Override
    public FrequentElements<Long> merge(FrequentElements<Long> other) {
        assert other instanceof LongFrequentElements: "Cannot merge instance of type "+ other.getClass();
        return merge((LongFrequentElements)other);
    }

    public LongFrequentElements merge(LongFrequentElements other) {
        NavigableSet<LongFrequencyEstimate> merged = this.elements;
        NavigableSet<LongFrequencyEstimate> otherEstimates = other.elements;
        /*
         * We need to pick out *maxSize* most common elements from both sets. Since this
         * is relatively small data, we will maintain a set in occurrance order, and add everything
         * from both sets, merging as we go. In order to effectively do that, we use a simple array,
         * add everything in, then add back in the number of elements that we need.
         */
        int totalSize = merged.size()+otherEstimates.size();
        LongFrequencyEstimate[] topK = new LongFrequencyEstimate[totalSize]; //assume no intersection
        int size =0;
        for(LongFrequencyEstimate estimate:merged){
            topK[size] = estimate;
            size++;
        }
        for(LongFrequencyEstimate otherEst:otherEstimates){
            boolean found = false;
            for(int i=0;i<size;i++){
                LongFrequencyEstimate existingEst = topK[i];
                if(existingEst.equals(otherEst)){
                    topK[i] = (LongFrequencyEstimate)existingEst.merge(otherEst);
                    found=true;
                    break;
                }
            }
            if(!found) {
                topK[size] = otherEst;
                size++;
            }
        }
        this.totalCount+=other.totalCount;
        this.elements = rebuild(totalCount,topK);

        return this;
    }

    @Override public FrequentElements<Long> getClone() { return newCopy(); }

    public LongFrequentElements newCopy() {
        Collection<LongFrequencyEstimate> ests = new TreeSet<>(naturalComparator);
        for(LongFrequencyEstimate est:elements){
            ests.add(new LongValueEstimate(est.value(),est.count(),est.error()));
        }
        return getNew(totalCount,ests);
    }

    protected abstract NavigableSet<LongFrequencyEstimate> rebuild(long mergedCount,LongFrequencyEstimate[] topK);

    protected abstract LongFrequentElements getNew(long totalCount, Collection<LongFrequencyEstimate> ests);

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static class TopK extends LongFrequentElements{
        private final int k;
        private static final Comparator<? super LongFrequencyEstimate> frequencyComparator = new Comparator<LongFrequencyEstimate>() {
            @Override
            public int compare(LongFrequencyEstimate o1, LongFrequencyEstimate o2) {
                int compare = Longs.compare(o1.count(), o2.count());
                if(compare!=0)
                    return compare;
                return o1.compareTo(o2);
            }
        };

        private TopK(int k,long totalCount, Collection<LongFrequencyEstimate> elements) {
            super(totalCount, elements);
            this.k = k;
        }

        @Override
        protected NavigableSet<LongFrequencyEstimate> rebuild(long mergedCount, LongFrequencyEstimate[] topK) {
            Arrays.sort(topK,frequencyComparator);
            int k = Math.min(this.k,topK.length);
            NavigableSet<LongFrequencyEstimate> newElements = new TreeSet<>(naturalComparator);
            //noinspection ManualArrayToCollectionCopy
            for(int i=0;i<k;i++){
                newElements.add(topK[i]);
            }
            return newElements;
        }

        @Override
        protected LongFrequentElements getNew(long totalCount, Collection<LongFrequencyEstimate> ests) {
            return new TopK(k,totalCount,ests);
        }
    }

    private static class HeavyItems extends LongFrequentElements{
        private final float support;
        private HeavyItems(float support,long totalCount, Collection<LongFrequencyEstimate> elements) {
            super(totalCount, elements);
            this.support = support;
        }

        @Override
        protected NavigableSet<LongFrequencyEstimate> rebuild(long mergedCount, LongFrequencyEstimate[] topK) {
            NavigableSet<LongFrequencyEstimate> result = new TreeSet<>(naturalComparator);
            long threshold = (long)(mergedCount*support);
            //noinspection ForLoopReplaceableByForEach
            for(int i=0;i< topK.length;i++){
                LongFrequencyEstimate est = topK[i];
                if(est.count()>threshold)
                    result.add(est);
            }
            return result;
        }

        @Override
        protected LongFrequentElements getNew(long totalCount, Collection<LongFrequencyEstimate> ests) {
            return new HeavyItems(support,totalCount,ests);
        }
    }

    private class ZeroFreq implements LongFrequencyEstimate {
        private long item;

        public ZeroFreq(long item) {
            this.item = item;
        }

        @Override public long value() { return item; }
        @SuppressWarnings("NullableProblems")
        @Override public int compareTo(LongFrequencyEstimate o) { return Longs.compare(item,o.value()); }
        @Override public Long getValue() { return item; }
        @Override public long count() { return 0; }
        @Override public long error() { return 0; }

        @Override public FrequencyEstimate<Long> merge(FrequencyEstimate<Long> other) { return other; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LongFrequencyEstimate)) return false;

            LongFrequencyEstimate zeroFreq = (LongFrequencyEstimate) o;

            return item == zeroFreq.value();
        }

        @Override
        public int hashCode() {
            return (int) (item ^ (item >>> 32));
        }
    }

    static class EncoderDecoder implements Encoder<LongFrequentElements> {
        public static final EncoderDecoder INSTANCE = new EncoderDecoder(); //singleton pattern

        @Override
        public void encode(LongFrequentElements item, DataOutput dataInput) throws IOException {
            dataInput.writeLong(item.totalCount);
            encodeSet(item.elements,dataInput);
            if(item instanceof TopK) {
                dataInput.writeBoolean(true);
                dataInput.writeInt(((TopK)item).k);
            }else {
                dataInput.writeBoolean(false);
                dataInput.writeFloat(((HeavyItems)item).support);
            }

        }

        @Override
        public LongFrequentElements decode(DataInput input) throws IOException {
            long totalCount = input.readLong();
            Set<LongFrequencyEstimate> estimates = decodeSet(input);
            if(input.readBoolean()){
                int k = input.readInt();
                return new TopK(k,totalCount,estimates);
            }else{
                float support = input.readFloat();
                return new HeavyItems(support,totalCount,estimates);
            }
        }

        private void encodeSet(NavigableSet<LongFrequencyEstimate> elements, DataOutput dataInput) throws IOException {
            dataInput.writeInt(elements.size());
            for(LongFrequencyEstimate element:elements){
                dataInput.writeLong(element.value());
                dataInput.writeLong(element.count());
                dataInput.writeLong(element.error());
            }
        }

        private Set<LongFrequencyEstimate> decodeSet(DataInput input) throws IOException{
            int size = input.readInt();
            Set<LongFrequencyEstimate> set = new TreeSet<>(naturalComparator);
            for(int i=0;i<size;i++){
                long v = input.readLong();
                long c = input.readLong();
                long eps = input.readLong();
                set.add(new LongValueEstimate(v,c,eps));
            }
            return set;
        }
    }
}
