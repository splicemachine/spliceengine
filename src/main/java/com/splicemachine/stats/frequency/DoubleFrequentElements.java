package com.splicemachine.stats.frequency;

import com.google.common.primitives.Doubles;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public class DoubleFrequentElements implements FrequentElements<Double>{
    private final NavigableSet<DoubleFrequencyEstimate> elements;
    private static final Comparator<DoubleFrequencyEstimate> naturalComparator = new Comparator<DoubleFrequencyEstimate>() {
        @Override
        public int compare(DoubleFrequencyEstimate o1, DoubleFrequencyEstimate o2) {
            int compare = Doubles.compare(o1.value(), o2.value());
            if(compare!=0) return compare;
            return Long.signum(o1.count()-o2.count()); //should never happen, but just in case
        }
    };

    public DoubleFrequentElements(Collection<DoubleFrequencyEstimate> elements) {
        TreeSet<DoubleFrequencyEstimate> elems = new TreeSet<DoubleFrequencyEstimate>(naturalComparator);
        elems.addAll(elements);
        this.elements = elems;
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
    public Set<? extends FrequencyEstimate<Double>> frequentElementsBetween(Double start, Double stop, boolean includeMin, boolean includeStop) {
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
