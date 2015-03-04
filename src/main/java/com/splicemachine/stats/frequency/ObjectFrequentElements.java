package com.splicemachine.stats.frequency;

import com.google.common.primitives.Longs;
import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.Mergeable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/8/14
 */
public abstract class ObjectFrequentElements<T> implements FrequentElements<T>,Mergeable<FrequentElements<T>> {


    protected final Comparator<? super FrequencyEstimate<T>> frequencyComparator = new Comparator<FrequencyEstimate<T>>() {
        @Override
        public int compare(FrequencyEstimate<T> o1, FrequencyEstimate<T> o2) {
            int compare = Longs.compare(o1.count(), o2.count());
            if(compare!=0)
                return compare;
            return comparator.compare(o1.getValue(),o2.getValue());
        }
    };
    private NavigableSet<FrequencyEstimate<T>> elements;
    private Comparator<? super T> comparator;
    private long totalCount;

    protected final Comparator<FrequencyEstimate<T>> naturalComparator;

    /*Private so that we can do polymorphism transparently*/
    private ObjectFrequentElements(long totalCount,
                                   Collection<FrequencyEstimate<T>> elements,
                                   Comparator<? super T> comparator) {
        this.totalCount = totalCount;
        this.comparator = comparator;
        this.naturalComparator = naturalComparator(comparator);
        this.elements = new TreeSet<>(naturalComparator);
        this.elements.addAll(elements);
    }

    /* ********************************************************************************************/
    /*Constructors*/
    public static <E> ObjectFrequentElements<E> topK(int k, long totalCount,
                                                     Collection<FrequencyEstimate<E>> elements,
                                                     Comparator<? super E> comparator){
        return new TopK<>(k,totalCount,elements,comparator);
    }

    public static <E> ObjectFrequentElements<E> heavyHitters(float support, long totalCount,
                                                     Collection<FrequencyEstimate<E>> elements,
                                                     Comparator<? super E> comparator){
        return new HeavyItems<>(support,totalCount,elements,comparator);
    }

    /* *********************************************************************************************/
    /*Accessors*/

    @Override
    public Set<? extends FrequencyEstimate<T>> allFrequentElements() {
        return Collections.unmodifiableSet(elements);
    }

    @Override
    public FrequencyEstimate<T> equal(T item) {
        for(FrequencyEstimate<T> n:elements){
            if(n.getValue().equals(item))
                return n;
        }
        return new ZeroFreq(item);
    }

    @Override
    public Set<? extends FrequencyEstimate<T>> frequentElementsBetween(T start,
                                                                       T stop,
                                                                       boolean includeMin, boolean includeStop) {
        if(start==null){
            if(stop==null){
                return Collections.unmodifiableSet(elements);
            }else
                return before(stop, includeStop);
        }else if(stop==null){
            return after(start, includeMin);
        }

        if (start.equals(stop)) {
            if (includeMin || includeStop) return Collections.singleton(equal(start));
            else return Collections.emptySet();
        }

        FrequencyEstimate<T> first = elements.first();
        int compare = comparator.compare(first.getValue(),stop);
        if(compare>0 || (!includeStop && compare==0)){
            /*
             * the start of our known range happens after the stop of the requested range,
             * so it's empty
             */
            return Collections.emptySet();
        }else if(includeStop && compare==0){
            return Collections.singleton(first);
        }

        compare = comparator.compare(first.getValue(),start);
        if(compare>0||(includeMin && compare==0))
            return before(stop,includeStop);

        FrequencyEstimate<T> last = elements.last();
        compare = comparator.compare(last.getValue(),start);
        if(compare<0 || (!includeMin && compare==0)){
            return Collections.emptySet();
        }else if(includeMin && compare==0){
            return Collections.singleton(last);
        }

        compare = comparator.compare(last.getValue(),stop);
        if(compare<0||(includeStop &&compare==0))
            return after(start,includeMin);

        ZeroFreq s = new ZeroFreq(start);
        FrequencyEstimate<T> startEst = elements.ceiling(s);
        s.item = stop;
        FrequencyEstimate<T> stopEst = elements.ceiling(s);
        return Collections.unmodifiableSet(elements.subSet(startEst,includeMin,stopEst,includeStop));
    }

    @Override
    @SuppressWarnings("unchecked")
    public FrequentElements<T> merge(FrequentElements<T> other) {
        assert other instanceof ObjectFrequentElements: "Cannot merge elements of type "+ other.getClass();
        ObjectFrequentElements<T> oE = (ObjectFrequentElements<T>)other;
        NavigableSet<FrequencyEstimate<T>> merged = this.elements;
        NavigableSet<FrequencyEstimate<T>> otherEstimates = oE.elements;
        /*
         * We need to pick out *maxSize* most common elements from both sets. Since this
         * is relatively small data, we will maintain a set in occurrance order, and add everything
         * from both sets, merging as we go. In order to effectively do that, we use a simple array,
         * add everything in, then add back in the number of elements that we need.
         */
        int totalSize = merged.size()+otherEstimates.size();
        FrequencyEstimate[] topK = new FrequencyEstimate[totalSize]; //assume no intersection
        int size =0;
        for(FrequencyEstimate<T> estimate:merged){
            topK[size] = estimate;
            size++;
        }
        for(FrequencyEstimate<T> otherEst:otherEstimates){
            boolean found = false;
            for(int i=0;i<size;i++){
                FrequencyEstimate<T> existingEst = topK[i];
                if(existingEst.equals(otherEst)){
                    topK[i] = existingEst.merge(otherEst);
                    found=true;
                    break;
                }
            }
            if(!found) {
                topK[size] = otherEst;
                size++;
            }
        }
        this.totalCount+=oE.totalCount;
        this.elements = rebuild(totalCount,topK);

        return this;
    }


    @Override
    public FrequentElements<T> getClone() {
        Collection<FrequencyEstimate<T>> copy = new TreeSet<>(naturalComparator);
        for(FrequencyEstimate<T> est:elements){
            copy.add(new ValueEstimate<>(est.getValue(),est.count(),est.error(),comparator));
        }
        return getNew(totalCount,copy,comparator);
    }

    protected abstract NavigableSet<FrequencyEstimate<T>> rebuild(long mergedCount,FrequencyEstimate<T>[] topK);

    protected abstract FrequentElements<T> getNew(long totalCount, Collection<FrequencyEstimate<T>> copy,Comparator<? super T> comparator);

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static class TopK<E> extends ObjectFrequentElements<E>{
        private final int k;

        private TopK(int k,long totalCount, Collection<FrequencyEstimate<E>> elements,Comparator<? super E> comparator) {
            super(totalCount, elements,comparator);
            this.k = k;
        }

        @Override
        protected NavigableSet<FrequencyEstimate<E>> rebuild(long mergedCount, FrequencyEstimate<E>[] topK) {
            Arrays.sort(topK,frequencyComparator);
            int k = Math.min(this.k,topK.length);
            NavigableSet<FrequencyEstimate<E>> newElements = new TreeSet<>(naturalComparator);
            //noinspection ManualArrayToCollectionCopy
            for(int i=0;i<k;i++){
                newElements.add(topK[i]);
            }
            return newElements;
        }

        @Override
        protected FrequentElements<E> getNew(long totalCount, Collection<FrequencyEstimate<E>> copy,Comparator<? super E> comparator) {
            return new TopK<>(k,totalCount,copy,comparator);
        }
    }

    private static class HeavyItems<E> extends ObjectFrequentElements<E>{
        private final float support;
        private HeavyItems(float support,long totalCount, Collection<FrequencyEstimate<E>> elements,Comparator<? super E> comparator) {
            super(totalCount, elements,comparator);
            this.support = support;
        }

        @Override
        protected NavigableSet<FrequencyEstimate<E>> rebuild(long mergedCount, FrequencyEstimate<E>[] topK) {
            NavigableSet<FrequencyEstimate<E>> result = new TreeSet<>(naturalComparator);
            long threshold = (long)(mergedCount*support);
            //noinspection ForLoopReplaceableByForEach
            for(int i=0;i< topK.length;i++){
                FrequencyEstimate<E> est = topK[i];
                if(est.count()>threshold)
                    result.add(est);
            }
            return result;
        }

        @Override
        protected FrequentElements<E> getNew(long totalCount, Collection<FrequencyEstimate<E>> copy, Comparator<? super E> comparator) {
            return new HeavyItems<>(support,totalCount,copy,comparator);
        }
    }

    private Set<? extends FrequencyEstimate<T>> after(T start, boolean includeMin) {
        FrequencyEstimate<T> last = elements.last();
        int lastCompare = comparator.compare(last.getValue(),start);
        if(lastCompare<0 || (!includeMin && lastCompare==0)) return Collections.emptySet();
        else if(includeMin && lastCompare==0) return Collections.singleton(last);

        FrequencyEstimate<T> first = elements.first();
        int firstCompare = comparator.compare(first.getValue(),start);
        if(firstCompare>0||(includeMin && firstCompare==0)) return Collections.unmodifiableSet(elements);

        ZeroFreq s = new ZeroFreq(start);
        FrequencyEstimate<T> startRange = elements.ceiling(s);
        return Collections.unmodifiableSet(elements.tailSet(startRange, includeMin));
    }

    private Set<? extends FrequencyEstimate<T>> before(T stop, boolean includeStop) {
        FrequencyEstimate<T> first = elements.first();
        int firstCompare = comparator.compare(first.getValue(),stop);
        if(firstCompare>0||(!includeStop && firstCompare==0)) return Collections.emptySet();
        else if(includeStop && firstCompare==0) return Collections.singleton(first);

        FrequencyEstimate<T> last = elements.last();
        int lastCompare = comparator.compare(last.getValue(),stop);
        if(lastCompare<0||(includeStop && lastCompare==0)) return Collections.unmodifiableSet(elements);
        else if(!includeStop && lastCompare==0) return Collections.unmodifiableSet(elements.headSet(last, false));

        ZeroFreq s = new ZeroFreq(stop);
        FrequencyEstimate<T> endRange = elements.ceiling(s);
        return Collections.unmodifiableSet(elements.headSet(endRange,includeStop));
    }

    private class ZeroFreq implements FrequencyEstimate<T> {
        private T item;

        public ZeroFreq(T item) {
            this.item = item;
        }

        @Override
        public T getValue() {
            return item;
        }

        @Override public long count() { return 0; }
        @Override public long error() { return 0; }

        @Override public FrequencyEstimate<T> merge(FrequencyEstimate<T> other) { return other; }
    }

    private static <T> Comparator<FrequencyEstimate<T>> naturalComparator(final Comparator<? super T> comparator) {
        return new Comparator<FrequencyEstimate<T>>() {
            @Override
            public int compare(FrequencyEstimate<T> o1, FrequencyEstimate<T> o2) {
                int compare = comparator.compare(o1.getValue(),o2.getValue());
                if(compare!=0) return compare;
                return Long.signum(o1.count()-o2.count()); //should never happen, but just in case
            }
        };
    }


    static class EncoderDecoder<T> implements Encoder<FrequentElements<T>> {
        private final Comparator<? super T> comparator;
        private final Encoder<T> valueEncoder;

        public EncoderDecoder(Encoder<T> valueEncoder, Comparator<? super T> comparator) {
            this.valueEncoder = valueEncoder;
            this.comparator = comparator;
        }

        @Override
        public void encode(FrequentElements<T> item, DataOutput dataInput) throws IOException {
            assert item instanceof ObjectFrequentElements: " cannot encode elements of type "+ item.getClass();
            ObjectFrequentElements<T> oE = (ObjectFrequentElements<T>)item;
            dataInput.writeLong(oE.totalCount);
            encodeSet(oE.elements,dataInput);
            if(item instanceof TopK) {
                dataInput.writeBoolean(true);
                dataInput.writeInt(((TopK)item).k);
            }else {
                dataInput.writeBoolean(false);
                dataInput.writeFloat(((HeavyItems)item).support);
            }
        }

        @Override
        public ObjectFrequentElements<T> decode(DataInput input) throws IOException {
            long totalCount = input.readLong();
            Set<FrequencyEstimate<T>> estimates = decodeSet(input);
            if(input.readBoolean()){
                int k = input.readInt();
                return new TopK<>(k,totalCount,estimates,comparator);
            }else{
                float support = input.readFloat();
                return new HeavyItems<>(support,totalCount,estimates,comparator);
            }
        }

        private void encodeSet(NavigableSet<FrequencyEstimate<T>> elements, DataOutput dataInput) throws IOException {
            dataInput.writeInt(elements.size());
            for(FrequencyEstimate<T> element:elements){
                valueEncoder.encode(element.getValue(), dataInput);
                dataInput.writeLong(element.count());
                dataInput.writeLong(element.error());
            }
        }

        private Set<FrequencyEstimate<T>> decodeSet(DataInput input) throws IOException{
            int size = input.readInt();
            Set<FrequencyEstimate<T>> set = new TreeSet<>(naturalComparator(comparator));
            for(int i=0;i<size;i++){
                T v = valueEncoder.decode(input);
                long c = input.readLong();
                long eps = input.readLong();
                set.add(new ValueEstimate<>(v,c,eps,comparator));
            }
            return set;
        }
    }
}
