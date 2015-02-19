package com.splicemachine.stats.frequency;

import com.google.common.primitives.Ints;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public class IntFrequentElements implements FrequentElements<Integer> {
    private final NavigableSet<IntFrequencyEstimate> elements;
    private static final Comparator<IntFrequencyEstimate> naturalComparator = new Comparator<IntFrequencyEstimate>() {
        @Override
        public int compare(IntFrequencyEstimate o1, IntFrequencyEstimate o2) {
            int compare = Ints.compare(o1.value(), o2.value());
            if(compare!=0) return compare;
            return Long.signum(o1.count()-o2.count()); //should never happen, but just in case
        }
    };

    public IntFrequentElements(Collection<IntFrequencyEstimate> elements) {
        TreeSet<IntFrequencyEstimate> elems = new TreeSet<IntFrequencyEstimate>(naturalComparator);
        elems.addAll(elements);
        this.elements = elems;
    }

    public IntFrequencyEstimate countEqual(int item){
        for(IntFrequencyEstimate est:elements){
            if(est.value()==item) return est;
        }
        return new ZeroFreq(item);

    }

    public Set<IntFrequencyEstimate> frequentBetween(int start, int stop, boolean includeStart, boolean includeStop){
        IntFrequencyEstimate first = elements.first();
        if(first.value()>stop) return Collections.emptySet();
        else if(first.value()==stop){
            if(includeStop) return Collections.singleton(first);
            else return Collections.emptySet();
        }else if(first.value()>start || (includeStart && first.value()==start))
            return frequentBefore(stop,includeStop);

        IntFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }else if(last.value()<stop || (includeStop && last.value()==stop)) return frequentAfter(start,includeStart);

        ZeroFreq est = new ZeroFreq(start);
        IntFrequencyEstimate rangeStart = elements.ceiling(est);
        est.item = stop;
        IntFrequencyEstimate rangeStop = elements.ceiling(est);
        return Collections.unmodifiableSet(elements.subSet(rangeStart,includeStart,rangeStop,includeStop));
    }

    public Set<IntFrequencyEstimate> frequentAfter(int start, boolean includeStart){
        IntFrequencyEstimate first = elements.first();
        if(first.value()>start) return elements;
        else if(first.value()==start){
            if(includeStart) return elements;
            else return elements.tailSet(first,false);
        }
        IntFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }

        //find the starting value
        IntFrequencyEstimate rangeStart = elements.ceiling(new ZeroFreq(start));
        return Collections.unmodifiableSet(elements.tailSet(rangeStart,includeStart));
    }

    public Set<IntFrequencyEstimate> frequentBefore(int stop, boolean includeStop){
        IntFrequencyEstimate first = elements.first();
        if(first.value()>stop)return Collections.emptySet();
        else if(first.value()==stop){
            if(!includeStop) return Collections.emptySet();
            else return Collections.singleton(first);
        }
        IntFrequencyEstimate last = elements.last();
        if(last.value()<stop) return elements;
        else if(last.value()==stop){
            if(includeStop) return elements;
            else return elements.headSet(last);
        }

        IntFrequencyEstimate rangeStop = elements.higher(new ZeroFreq(stop));
        return Collections.unmodifiableSet(elements.headSet(rangeStop,includeStop));
    }

    @Override
    public FrequencyEstimate<? extends Integer> equal(Integer item) {
        assert item!=null: "Cannot estimate null matches!";
        return countEqual(item);
    }

    @Override
    public Set<? extends FrequencyEstimate<Integer>> frequentElementsBetween(Integer start, Integer stop, boolean includeMin, boolean includeStop) {
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

    private class ZeroFreq implements IntFrequencyEstimate {
        private int item;

        public ZeroFreq(int item) {
            this.item = item;
        }

        @Override public int value() { return item; }
        @SuppressWarnings("NullableProblems")
        @Override public int compareTo(IntFrequencyEstimate o) { return Ints.compare(item, o.value()); }
        @Override public Integer getValue() { return item; }
        @Override public long count() { return 0; }
        @Override public long error() { return 0; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof IntFrequencyEstimate)) return false;

            IntFrequencyEstimate zeroFreq = (IntFrequencyEstimate) o;

            return item == zeroFreq.value();
        }

        @Override public int hashCode() { return item; }
    }
}
