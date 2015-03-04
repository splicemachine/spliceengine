package com.splicemachine.stats.frequency;

import com.google.common.primitives.Doubles;
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
public abstract class DoubleFrequentElements implements FrequentElements<Double> {
    private static final Comparator<DoubleFrequencyEstimate> naturalComparator = new Comparator<DoubleFrequencyEstimate>() {
        @Override
        public int compare(DoubleFrequencyEstimate o1, DoubleFrequencyEstimate o2) {
            int compare = Doubles.compare(o1.value(), o2.value());
            if(compare!=0) return compare;
            return Long.signum(o1.count()-o2.count()); //should never happen, but just in case
        }
    };

    private NavigableSet<DoubleFrequencyEstimate> elements;
    private long totalCount;

    public static DoubleFrequentElements topK(int k, long totalCount,Collection<DoubleFrequencyEstimate> elements){
        return new TopK(k,totalCount,elements);
    }

    public static DoubleFrequentElements heavyHitters(float support,
                                                      long totalCount,Collection<DoubleFrequencyEstimate> elements){
        return new HeavyItems(support,totalCount,elements);
    }

    private DoubleFrequentElements(long totalCount,Collection<DoubleFrequencyEstimate> elements) {
        TreeSet<DoubleFrequencyEstimate> elems = new TreeSet<>(naturalComparator);
        elems.addAll(elements);
        this.elements = elems;
        this.totalCount = totalCount;
    }

    @Override
    public Set<? extends FrequencyEstimate<Double>> allFrequentElements() {
        return frequentBetween(elements.first().value(),elements.last().value(),true,true);
    }

    @Override
    public FrequentElements<Double> getClone() {
        return newCopy();
    }

    public DoubleFrequentElements newCopy() {
        Collection<DoubleFrequencyEstimate> copy = new TreeSet<>(naturalComparator);
        for(DoubleFrequencyEstimate est:elements){
            copy.add(new DoubleValueEstimate(est.value(),est.count(),est.error()));
        }
        return getNew(totalCount,copy);
    }


    public DoubleFrequencyEstimate countEqual(double item){
        for(DoubleFrequencyEstimate est:elements){
            if(est.value()==item) return est;
        }
        return new ZeroFreq(item);
    }

    public Set<DoubleFrequencyEstimate> frequentBetween(double start, double stop, boolean includeStart, boolean includeStop){
        DoubleFrequencyEstimate first = elements.first();
        if(first.value()>stop) return Collections.emptySet();
        else if(first.value()==stop){
            if(includeStop) return Collections.singleton(first);
            else return Collections.emptySet();
        }else if(first.value()>start || (includeStart && first.value()==start))
            return frequentBefore(stop,includeStop);

        DoubleFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }else if(last.value()<stop || (includeStop && last.value()==stop)) return frequentAfter(start,includeStart);

        ZeroFreq est = new ZeroFreq(start);
        DoubleFrequencyEstimate rangeStart = elements.ceiling(est);
        est.item = stop;
        DoubleFrequencyEstimate rangeStop = elements.ceiling(est);
        return Collections.unmodifiableSet(elements.subSet(rangeStart,includeStart,rangeStop,includeStop));
    }

    public Set<DoubleFrequencyEstimate> frequentAfter(double start, boolean includeStart){
        DoubleFrequencyEstimate first = elements.first();
        if(first.value()>start) return elements;
        else if(first.value()==start){
            if(includeStart) return elements;
            else return elements.tailSet(first,false);
        }
        DoubleFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }

        //find the starting value
        DoubleFrequencyEstimate rangeStart = elements.ceiling(new ZeroFreq(start));
        return Collections.unmodifiableSet(elements.tailSet(rangeStart,includeStart));
    }

    public Set<DoubleFrequencyEstimate> frequentBefore(double stop, boolean includeStop){
        DoubleFrequencyEstimate first = elements.first();
        if(first.value()>stop)return Collections.emptySet();
        else if(first.value()==stop){
            if(!includeStop) return Collections.emptySet();
            else return Collections.singleton(first);
        }
        DoubleFrequencyEstimate last = elements.last();
        if(last.value()<stop) return elements;
        else if(last.value()==stop){
            if(includeStop) return elements;
            else return elements.headSet(last);
        }

        DoubleFrequencyEstimate rangeStop = elements.higher(new ZeroFreq(stop));
        return Collections.unmodifiableSet(elements.headSet(rangeStop,includeStop));
    }

    @Override
    public FrequencyEstimate<? extends Double> equal(Double item) {
        assert item!=null: "Cannot estimate null matches!";
        return countEqual(item);
    }

    @Override
    public Set<? extends FrequencyEstimate<Double>> frequentElementsBetween(
            Double start,
            Double stop,
            boolean includeMin, boolean includeStop) {
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
    public FrequentElements<Double> merge(FrequentElements<Double> other) {
        assert other instanceof DoubleFrequentElements: "Cannot merge instance of type "+ other.getClass();
        return merge((DoubleFrequentElements)other);
    }

    public DoubleFrequentElements merge(DoubleFrequentElements other) {
        NavigableSet<DoubleFrequencyEstimate> thisEst = elements;
        NavigableSet<DoubleFrequencyEstimate> oEst = other.elements;

        DoubleFrequencyEstimate[] all = new DoubleFrequencyEstimate[thisEst.size()+oEst.size()];
        int size =0;
        for(DoubleFrequencyEstimate est:thisEst){
            all[size] = est;
            size++;
        }
        for(DoubleFrequencyEstimate est:oEst){
            boolean found = false;
            for(int i=0;i<size;i++){
                DoubleFrequencyEstimate thisE = all[i];
                if(thisE.equals(est)){
                    all[i] = (DoubleFrequencyEstimate)thisE.merge(est);
                    found = true;
                    break;
                }
            }
            if(!found){
                all[size] = est;
                size++;
            }
        }
        elements = rebuild(other.totalCount+totalCount,all);
        totalCount +=other.totalCount;
        return this;
    }

    protected abstract NavigableSet<DoubleFrequencyEstimate> rebuild(long totalCount, DoubleFrequencyEstimate[] all);

    protected abstract DoubleFrequentElements getNew(long totalCount, Collection<DoubleFrequencyEstimate> copy);

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static class TopK extends DoubleFrequentElements{
        private static final Comparator<DoubleFrequencyEstimate> frequencyComparator = new Comparator<DoubleFrequencyEstimate>() {
            @Override
            public int compare(DoubleFrequencyEstimate o1, DoubleFrequencyEstimate o2) {
                int compare = Longs.compare(o1.count(),o2.count());
                if(compare!=0) return compare;
                return o1.compareTo(o2);
            }
        };

        private int k;

        private TopK(int k,long totalCount, Collection<DoubleFrequencyEstimate> elements) {
            super(totalCount, elements);
            this.k = k;
        }

        @Override
        protected NavigableSet<DoubleFrequencyEstimate> rebuild(long totalCount, DoubleFrequencyEstimate[] all) {
            Arrays.sort(all,frequencyComparator);

            int k = Math.min(this.k,all.length);
            NavigableSet<DoubleFrequencyEstimate> newTree = new TreeSet<>(naturalComparator);
            //noinspection ManualArrayToCollectionCopy
            for(int i=0;i<k;i++){
                newTree.add(all[i]);
            }
            return newTree;
        }

        @Override
        protected DoubleFrequentElements getNew(long totalCount, Collection<DoubleFrequencyEstimate> copy) {
            return new TopK(k,totalCount,copy);
        }
    }

    private static class HeavyItems extends DoubleFrequentElements{
        private float support;

        private HeavyItems(float support,long totalCount, Collection<DoubleFrequencyEstimate> elements) {
            super(totalCount, elements);
            this.support = support;
        }


        @Override
        protected NavigableSet<DoubleFrequencyEstimate> rebuild(long totalCount, DoubleFrequencyEstimate[] all) {
            NavigableSet<DoubleFrequencyEstimate> result = new TreeSet<>(naturalComparator);
            long threshold = (long)(totalCount*support);
            //noinspection ForLoopReplaceableByForEach
            for(int i=0;i< all.length;i++){
                DoubleFrequencyEstimate est = all[i];
                if(est.count()>threshold)
                    result.add(est);
            }
            return result;
        }

        @Override
        protected DoubleFrequentElements getNew(long totalCount, Collection<DoubleFrequencyEstimate> copy) {
            return new HeavyItems(support,totalCount,copy);
        }
    }

    private class ZeroFreq implements DoubleFrequencyEstimate {
        private double item;

        public ZeroFreq(double item) {
            this.item = item;
        }

        @Override public double value() { return item; }
        @SuppressWarnings("NullableProblems")
        @Override public int compareTo(DoubleFrequencyEstimate o) { return Doubles.compare(item,o.value()); }
        @Override public Double getValue() { return item; }
        @Override public long count() { return 0; }
        @Override public long error() { return 0; }
        @Override public FrequencyEstimate<Double> merge(FrequencyEstimate<Double> other) { return other; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DoubleFrequencyEstimate)) return false;

            DoubleFrequencyEstimate zeroFreq = (DoubleFrequencyEstimate) o;

            return item == zeroFreq.value();
        }

        @Override public int hashCode() { return Doubles.hashCode(item); }
    }

    static class EncoderDecoder implements Encoder<DoubleFrequentElements> {
        public static final EncoderDecoder INSTANCE = new EncoderDecoder(); //singleton pattern

        @Override
        public void encode(DoubleFrequentElements item, DataOutput dataInput) throws IOException {
            dataInput.writeDouble(item.totalCount);
            encodeSet(item.elements,dataInput);
            if(item instanceof TopK) {
                dataInput.writeBoolean(true);
                dataInput.writeInt(((TopK)item).k);
            }else {
                dataInput.writeBoolean(false);
                dataInput.writeFloat(((HeavyItems) item).support);
            }
        }

        @Override
        public DoubleFrequentElements decode(DataInput input) throws IOException {
            long totalCount = input.readLong();
            Set<DoubleFrequencyEstimate> estimates = decodeSet(input);
            if(input.readBoolean()){
                int k = input.readInt();
                return new TopK(k,totalCount,estimates);
            }else{
                float support = input.readFloat();
                return new HeavyItems(support,totalCount,estimates);
            }
        }

        private void encodeSet(NavigableSet<DoubleFrequencyEstimate> elements, DataOutput dataInput) throws IOException {
            dataInput.writeInt(elements.size());
            for(DoubleFrequencyEstimate element:elements){
                dataInput.writeDouble(element.value());
                dataInput.writeLong(element.count());
                dataInput.writeLong(element.error());
            }
        }

        private Set<DoubleFrequencyEstimate> decodeSet(DataInput input) throws IOException {
            int size = input.readInt();
            Set<DoubleFrequencyEstimate> set = new TreeSet<>(naturalComparator);
            for(int i=0;i<size;i++){
                double v = input.readDouble();
                long c = input.readLong();
                long eps = input.readLong();
                set.add(new DoubleValueEstimate(v,c,eps));
            }
            return set;
        }
    }
}
