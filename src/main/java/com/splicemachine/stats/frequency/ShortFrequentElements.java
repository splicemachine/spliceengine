package com.splicemachine.stats.frequency;

import com.google.common.primitives.Shorts;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public class ShortFrequentElements implements FrequentElements<Short> {
    private final NavigableSet<ShortFrequencyEstimate> elements;
    private static final Comparator<ShortFrequencyEstimate> naturalComparator = new Comparator<ShortFrequencyEstimate>() {
        @Override
        public int compare(ShortFrequencyEstimate o1, ShortFrequencyEstimate o2) {
            int compare = Shorts.compare(o1.value(), o2.value());
            if(compare!=0) return compare;
            return Long.signum(o1.count()-o2.count()); //should never happen, but just in case
        }
    };

    public ShortFrequentElements(Collection<ShortFrequencyEstimate> elements) {
        TreeSet<ShortFrequencyEstimate> elems = new TreeSet<ShortFrequencyEstimate>(naturalComparator);
        elems.addAll(elements);
        this.elements = elems;
    }

    public ShortFrequencyEstimate countEqual(short item){
        for(ShortFrequencyEstimate est:elements){
            if(est.value()==item) return est;
        }
        return new ZeroFreq(item);

    }

    public Set<ShortFrequencyEstimate> frequentBetween(short start, short stop, boolean includeStart, boolean includeStop){
        ShortFrequencyEstimate first = elements.first();
        if(first.value()>stop) return Collections.emptySet();
        else if(first.value()==stop){
            if(includeStop) return Collections.singleton(first);
            else return Collections.emptySet();
        }else if(first.value()>start || (includeStart && first.value()==start))
            return frequentBefore(stop,includeStop);

        ShortFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }else if(last.value()<stop || (includeStop && last.value()==stop)) return frequentAfter(start,includeStart);

        ZeroFreq est = new ZeroFreq(start);
        ShortFrequencyEstimate rangeStart = elements.ceiling(est);
        est.item = stop;
        ShortFrequencyEstimate rangeStop = elements.ceiling(est);
        return Collections.unmodifiableSet(elements.subSet(rangeStart,includeStart,rangeStop,includeStop));
    }

    public Set<ShortFrequencyEstimate> frequentAfter(short start, boolean includeStart){
        ShortFrequencyEstimate first = elements.first();
        if(first.value()>start) return elements;
        else if(first.value()==start){
            if(includeStart) return elements;
            else return elements.tailSet(first,false);
        }
        ShortFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }

        //find the starting value
        ShortFrequencyEstimate rangeStart = elements.ceiling(new ZeroFreq(start));
        return Collections.unmodifiableSet(elements.tailSet(rangeStart,includeStart));
    }

    public Set<ShortFrequencyEstimate> frequentBefore(short stop, boolean includeStop){
        ShortFrequencyEstimate first = elements.first();
        if(first.value()>stop)return Collections.emptySet();
        else if(first.value()==stop){
            if(!includeStop) return Collections.emptySet();
            else return Collections.singleton(first);
        }
        ShortFrequencyEstimate last = elements.last();
        if(last.value()<stop) return elements;
        else if(last.value()==stop){
            if(includeStop) return elements;
            else return elements.headSet(last);
        }

        ShortFrequencyEstimate rangeStop = elements.higher(new ZeroFreq(stop));
        return Collections.unmodifiableSet(elements.headSet(rangeStop,includeStop));
    }

    @Override
    public FrequencyEstimate<? extends Short> equal(Short item) {
        assert item!=null: "Cannot estimate null matches!";
        return countEqual(item);
    }

    @Override
    public Set<? extends FrequencyEstimate<Short>> frequentElementsBetween(Short start, Short stop, boolean includeMin, boolean includeStop) {
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

    private class ZeroFreq implements ShortFrequencyEstimate {
        private short item;

        public ZeroFreq(short item) {
            this.item = item;
        }

        @Override public short value() { return item; }
        @SuppressWarnings("NullableProblems")
        @Override public int compareTo(ShortFrequencyEstimate o) { return Shorts.compare(item,o.value()); }
        @Override public Short getValue() { return item; }
        @Override public long count() { return 0; }
        @Override public long error() { return 0; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ShortFrequencyEstimate)) return false;

            ShortFrequencyEstimate zeroFreq = (ShortFrequencyEstimate) o;

            return item == zeroFreq.value();
        }

        @Override public int hashCode() { return item; }
    }
}
