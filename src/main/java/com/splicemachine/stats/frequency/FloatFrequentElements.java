package com.splicemachine.stats.frequency;

import com.google.common.primitives.Floats;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public class FloatFrequentElements implements FrequentElements<Float> {
    private final NavigableSet<FloatFrequencyEstimate> elements;
    private static final Comparator<FloatFrequencyEstimate> naturalComparator = new Comparator<FloatFrequencyEstimate>() {
        @Override
        public int compare(FloatFrequencyEstimate o1, FloatFrequencyEstimate o2) {
            int compare = Floats.compare(o1.value(), o2.value());
            if(compare!=0) return compare;
            return Long.signum(o1.count()-o2.count()); //should never happen, but just in case
        }
    };

    public FloatFrequentElements(Collection<FloatFrequencyEstimate> elements) {
        TreeSet<FloatFrequencyEstimate> elems = new TreeSet<FloatFrequencyEstimate>(naturalComparator);
        elems.addAll(elements);
        this.elements = elems;
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
            else return elements.tailSet(first,false);
        }
        FloatFrequencyEstimate last = elements.last();
        if(last.value()<start) return Collections.emptySet();
        else if(last.value()==start){
            if(includeStart) return Collections.singleton(last);
            else return Collections.emptySet();
        }

        //find the starting value
        FloatFrequencyEstimate rangeStart = elements.ceiling(new ZeroFreq(start));
        return Collections.unmodifiableSet(elements.tailSet(rangeStart,includeStart));
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
        return Collections.unmodifiableSet(elements.headSet(rangeStop,includeStop));
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FloatFrequencyEstimate)) return false;

            FloatFrequencyEstimate zeroFreq = (FloatFrequencyEstimate) o;

            return item == zeroFreq.value();
        }

        @Override public int hashCode() { return Floats.hashCode(item); }
    }
}
