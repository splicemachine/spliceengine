package com.splicemachine.stats.frequency;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import com.splicemachine.encoding.Encoder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public abstract class FloatFrequentElements implements FrequentElements<Float> {
    private static final Comparator<FloatFrequencyEstimate> naturalComparator = new Comparator<FloatFrequencyEstimate>() {
        @Override
        public int compare(FloatFrequencyEstimate o1, FloatFrequencyEstimate o2) {
            int compare = Floats.compare(o1.value(), o2.value());
            if(compare!=0) return compare;
            return Long.signum(o1.count()-o2.count()); //should never happen, but just in case
        }
    };

    private NavigableSet<FloatFrequencyEstimate> elements;
    private long totalCount;

    private FloatFrequentElements(long totalCount,Collection<FloatFrequencyEstimate> elements) {
        TreeSet<FloatFrequencyEstimate> elems = new TreeSet<>(naturalComparator);
        elems.addAll(elements);
        this.elements = elems;
        this.totalCount = totalCount;
    }

    public static FloatFrequentElements topK(int k, long totalCount,Collection<FloatFrequencyEstimate> elements){
        return new TopK(k,totalCount,elements);
    }

    public static FloatFrequentElements heavyHitters(float support, long totalCount,Collection<FloatFrequencyEstimate> elements){
        return new HeavyItems(support,totalCount,elements);
    }

    @Override
    public Set<? extends FrequencyEstimate<Float>> allFrequentElements() {
        return Collections.unmodifiableSet(elements);
    }

    public FloatFrequencyEstimate countEqual(float item){
        for(FloatFrequencyEstimate est:elements){
            if(est.value()==item) return est;
        }
        return new ZeroFreq(item);

    }

    public Set<FloatFrequencyEstimate> frequentBetween(float start, float stop, boolean includeStart, boolean includeStop){
        FloatFrequencyEstimate first = elements.first();
        if(first.value()>stop) return Collections.emptySet();
        else if(first.value()==stop){
            if(includeStop) return Collections.singleton(first);
            else return Collections.emptySet();
        }else if(first.value()>start || (includeStart && first.value()==start))
            return frequentBefore(stop,includeStop);

        FloatFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }else if(last.value()<stop || (includeStop && last.value()==stop)) return frequentAfter(start,includeStart);

        ZeroFreq est = new ZeroFreq(start);
        FloatFrequencyEstimate rangeStart = elements.ceiling(est);
        est.item = stop;
        FloatFrequencyEstimate rangeStop = elements.ceiling(est);
        return Collections.unmodifiableSet(elements.subSet(rangeStart,includeStart,rangeStop,includeStop));
    }

    public Set<FloatFrequencyEstimate> frequentAfter(float start, boolean includeStart){
        FloatFrequencyEstimate first = elements.first();
        if(first.value()>start) return elements;
        else if(first.value()==start){
            if(includeStart) return elements;
            else return elements.tailSet(first, false);
        }
        FloatFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }

        //find the starting value
        FloatFrequencyEstimate rangeStart = elements.ceiling(new ZeroFreq(start));
        return Collections.unmodifiableSet(elements.tailSet(rangeStart, includeStart));
    }

    public Set<FloatFrequencyEstimate> frequentBefore(float stop, boolean includeStop){
        FloatFrequencyEstimate first = elements.first();
        if(first.value()>stop)return Collections.emptySet();
        else if(first.value()==stop){
            if(!includeStop) return Collections.emptySet();
            else return Collections.singleton(first);
        }
        FloatFrequencyEstimate last = elements.last();
        if(last.value()<stop) return elements;
        else if(last.value()==stop){
            if(includeStop) return elements;
            else return elements.headSet(last);
        }

        FloatFrequencyEstimate rangeStop = elements.higher(new ZeroFreq(stop));
        return Collections.unmodifiableSet(elements.headSet(rangeStop, includeStop));
    }

    @Override
    public FrequencyEstimate<? extends Float> equal(Float item) {
        assert item!=null: "Cannot estimate null matches!";
        return countEqual(item);
    }

    @Override
    public Set<? extends FrequencyEstimate<Float>> frequentElementsBetween(Float start, Float stop, boolean includeMin, boolean includeStop) {
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
    public FrequentElements<Float> merge(FrequentElements<Float> other) {
        assert other instanceof FloatFrequentElements: "Cannot merge instance of type "+ other.getClass();
        return merge((FloatFrequentElements)other);
    }

    //    @Override
    public FloatFrequentElements merge(FloatFrequentElements other) {
        NavigableSet<FloatFrequencyEstimate> merged = this.elements;
        NavigableSet<FloatFrequencyEstimate> otherEstimates = other.elements;
        /*
         * We need to pick out *maxSize* most common elements from both sets. Since this
         * is relatively small data, we will maintain a set in occurrance order, and add everything
         * from both sets, merging as we go. In order to effectively do that, we use a simple array,
         * add everything in, then add back in the number of elements that we need.
         */
        int totalSize = merged.size()+otherEstimates.size();
        FloatFrequencyEstimate[] topK = new FloatFrequencyEstimate[totalSize]; //assume no intersection
        int size =0;
        for(FloatFrequencyEstimate estimate:merged){
            topK[size] = estimate;
            size++;
        }
        for(FloatFrequencyEstimate otherEst:otherEstimates){
            boolean found = false;
            for(int i=0;i<size;i++){
                FloatFrequencyEstimate existingEst = topK[i];
                if(existingEst.equals(otherEst)){
                    topK[i] = (FloatFrequencyEstimate)existingEst.merge(otherEst);
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

    @Override public FrequentElements<Float> getClone() { return newCopy(); }

    public FloatFrequentElements newCopy() {
        Collection<FloatFrequencyEstimate> ests = new TreeSet<>(naturalComparator);
        for(FloatFrequencyEstimate est:elements){
            ests.add(new FloatValueEstimate(est.value(),est.count(),est.error()));
        }
        return getNew(totalCount,ests);
    }

    @Override
    public long totalFrequentElements() {
        long total = 0l;
        for(FrequencyEstimate<Float> freq:elements){
            total+=freq.count();
        }
        return total;
    }

    protected abstract NavigableSet<FloatFrequencyEstimate> rebuild(long mergedCount,FloatFrequencyEstimate[] topK);

    protected abstract FloatFrequentElements getNew(long totalCount, Collection<FloatFrequencyEstimate> ests);

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private static class TopK extends FloatFrequentElements{
        private final int k;
        private static final Comparator<FloatFrequencyEstimate> frequencyComparator = new Comparator<FloatFrequencyEstimate>() {
            @Override
            public int compare(FloatFrequencyEstimate o1, FloatFrequencyEstimate o2) {
                int compare = Longs.compare(o1.count(), o2.count());
                if(compare!=0) return compare;
                return o1.compareTo(o2);
            }
        };

        private TopK(int k,long totalCount, Collection<FloatFrequencyEstimate> elements) {
            super(totalCount, elements);
            this.k = k;
        }

        @Override
        protected NavigableSet<FloatFrequencyEstimate> rebuild(long mergedCount, FloatFrequencyEstimate[] topK) {
            Arrays.sort(topK,frequencyComparator);
            int k = Math.min(this.k,topK.length);
            NavigableSet<FloatFrequencyEstimate> newElements = new TreeSet<>(naturalComparator);
            //noinspection ManualArrayToCollectionCopy
            for(int i=0;i<k;i++){
                newElements.add(topK[i]);
            }
            return newElements;
        }

        @Override
        protected FloatFrequentElements getNew(long totalCount, Collection<FloatFrequencyEstimate> ests) {
            return new TopK(k,totalCount,ests);
        }

    }

    private static class HeavyItems extends FloatFrequentElements{
        private float support;

        private HeavyItems(float support,long totalCount, Collection<FloatFrequencyEstimate> elements) {
            super(totalCount, elements);
            this.support = support;
        }

        @Override
        protected NavigableSet<FloatFrequencyEstimate> rebuild(long mergedCount, FloatFrequencyEstimate[] topK) {
            NavigableSet<FloatFrequencyEstimate> result = new TreeSet<>(naturalComparator);
            long threshold = (long)(mergedCount*support);
            //noinspection ForLoopReplaceableByForEach
            for(int i=0;i< topK.length;i++){
                FloatFrequencyEstimate est = topK[i];
                if(est.count()>threshold)
                    result.add(est);
            }
            return result;
        }

        @Override
        protected FloatFrequentElements getNew(long totalCount, Collection<FloatFrequencyEstimate> ests) {
            return new HeavyItems(support,totalCount,ests);
        }
    }

    private class ZeroFreq implements FloatFrequencyEstimate {
        private float item;

        public ZeroFreq(float item) {
            this.item = item;
        }

        @Override public float value() { return item; }
        @SuppressWarnings("NullableProblems")
        @Override public int compareTo(FloatFrequencyEstimate o) { return Floats.compare(item,o.value()); }
        @Override public Float getValue() { return item; }
        @Override public long count() { return 0; }
        @Override public long error() { return 0; }

        @Override public FrequencyEstimate<Float> merge(FrequencyEstimate<Float> other) { return other; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FloatFrequencyEstimate)) return false;

            FloatFrequencyEstimate zeroFreq = (FloatFrequencyEstimate) o;

            return item == zeroFreq.value();
        }

        @Override public int hashCode() { return Floats.hashCode(item); }
    }
    static class EncoderDecoder implements Encoder<FloatFrequentElements> {
        public static final EncoderDecoder INSTANCE = new EncoderDecoder(); //singleton pattern

        @Override
        public void encode(FloatFrequentElements item, DataOutput dataInput) throws IOException {
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
        public FloatFrequentElements decode(DataInput input) throws IOException {
            long totalCount = input.readLong();
            Set<FloatFrequencyEstimate> estimates = decodeSet(input);
            if(input.readBoolean()){
                int k = input.readInt();
                return new TopK(k,totalCount,estimates);
            }else{
                float support = input.readFloat();
                return new HeavyItems(support,totalCount,estimates);
            }
        }

        private void encodeSet(NavigableSet<FloatFrequencyEstimate> elements, DataOutput dataInput) throws IOException {
            dataInput.writeInt(elements.size());
            for(FloatFrequencyEstimate element:elements){
                dataInput.writeFloat(element.value());
                dataInput.writeLong(element.count());
                dataInput.writeLong(element.error());
            }
        }

        private Set<FloatFrequencyEstimate> decodeSet(DataInput input) throws IOException{
            int size = input.readInt();
            Set<FloatFrequencyEstimate> set = new TreeSet<>(naturalComparator);
            for(int i=0;i<size;i++){
                float v = input.readFloat();
                long c = input.readLong();
                long eps = input.readLong();
                set.add(new FloatValueEstimate(v,c,eps));
            }
            return set;
        }
    }
}
