package com.splicemachine.stats.frequency;

import com.google.common.primitives.Longs;

import java.util.*;

/**
 * Long-specific representation of FrequentElements for a long.
 *
 * This allows primitive-specific queries against long values, without the need for autoboxing.
 *
 * @author Scott Fines
 *         Date: 12/5/14
 */
public class LongFrequentElements implements FrequentElements<Long> {
    private final NavigableSet<LongFrequencyEstimate> elements;
    private static final Comparator<LongFrequencyEstimate> naturalComparator = new Comparator<LongFrequencyEstimate>() {
        @Override
        public int compare(LongFrequencyEstimate o1, LongFrequencyEstimate o2) {
            int compare = Longs.compare(o1.value(),o2.value());
            if(compare!=0) return compare;
            return Long.signum(o1.count()-o2.count()); //should never happen, but just in case
        }
    };

    public LongFrequentElements(Collection<LongFrequencyEstimate> elements) {
        TreeSet<LongFrequencyEstimate> elems = new TreeSet<LongFrequencyEstimate>(naturalComparator);
        elems.addAll(elements);
        this.elements = elems;
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
            else return elements.tailSet(first,false);
        }
        LongFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }

        //find the starting value
        LongFrequencyEstimate rangeStart = elements.ceiling(new ZeroFreq(start));
        return Collections.unmodifiableSet(elements.tailSet(rangeStart,includeStart));
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
        return Collections.unmodifiableSet(elements.headSet(rangeStop,includeStop));
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
}
