package com.splicemachine.stats.frequency;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.splicemachine.stats.Mergeable;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public abstract class DoubleFrequentElements implements FrequentElements<Double>,Mergeable<DoubleFrequentElements> {
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
}
