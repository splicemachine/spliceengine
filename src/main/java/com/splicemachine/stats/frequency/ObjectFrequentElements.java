package com.splicemachine.stats.frequency;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/8/14
 */
public class ObjectFrequentElements<T> implements FrequentElements<T>{
    private final List<FrequencyEstimate<T>> elements;
    private final Comparator<? super T> comparator;
    private final Comparator<FrequencyEstimate<T>> freqComparator = new Comparator<FrequencyEstimate<T>>() {
        @Override
        public int compare(FrequencyEstimate<T> o1, FrequencyEstimate<T> o2) {
            int compare = comparator.compare(o1.getValue(),o2.getValue());
            if(compare!=0) return compare;
            return Long.signum(o1.count()-o2.count()); //should never happen, but just in case
        }
    };

    public ObjectFrequentElements(Collection<FrequencyEstimate<T>> elements,Comparator<? super T> comparator) {
        this.elements = Lists.newArrayList(elements);
        this.comparator = comparator;
    }

    @Override
    public FrequencyEstimate<T> equal(T item) {
        for(int i=0;i<elements.size();i++){
            FrequencyEstimate<T> n = elements.get(i);
            if(n.getValue().equals(item))
                return n;
        }
        return new ZeroFreq(item);
    }

    @Override
    public Set<? extends FrequencyEstimate<T>> frequentElementsBetween(T start, T stop, boolean includeMin, boolean includeStop) {
        if(start==null){
            if(stop==null){
                Set<FrequencyEstimate<T>> frequencyEstimates = Sets.newTreeSet(freqComparator);
                frequencyEstimates.addAll(elements);
                return frequencyEstimates;
            }else{
                return before(stop, includeStop);
            }
        }else if(stop==null){
            return after(start, includeMin);
        }else {
            if (start.equals(stop)) {
                if (includeMin || includeStop) return Collections.singleton(equal(start));
                else return Collections.emptySet();
            }
            Set<FrequencyEstimate<T>> frequencies = null;
            for (int i = 0; i < elements.size(); i++) {
                FrequencyEstimate<T> f = elements.get(i);
                int sCompare = comparator.compare(start, f.getValue());
                if (sCompare<0 ||(includeMin && sCompare == 0)){
                    int eCompare = comparator.compare(f.getValue(), stop);
                    if (eCompare<0 || (includeStop && eCompare == 0)){
                        if (frequencies == null)
                            frequencies = new TreeSet<FrequencyEstimate<T>>(freqComparator);
                        frequencies.add(f);
                    }
                }
            }

            if (frequencies == null) return Collections.emptySet();
            else return frequencies;
        }
    }

    private Set<? extends FrequencyEstimate<T>> after(T start, boolean includeMin) {
        Set<FrequencyEstimate<T>> frequencies = null;
        for(int i=0;i<elements.size();i++){
            FrequencyEstimate<T> f = elements.get(i);
            int sCompare = comparator.compare(start,f.getValue());
            if((includeMin && sCompare<=0)||(!includeMin && sCompare<0)){
                if(frequencies==null)
                    frequencies = new TreeSet<FrequencyEstimate<T>>(freqComparator);
                frequencies.add(f);
            }
        }

        if(frequencies==null) return Collections.emptySet();
        else return frequencies;
    }

    private Set<? extends FrequencyEstimate<T>> before(T stop, boolean includeStop) {
        Set<FrequencyEstimate<T>> frequencies = null;
        for(int i=0;i<elements.size();i++){
            FrequencyEstimate<T> f = elements.get(i);
            int eCompare = comparator.compare(f.getValue(),stop);
            if((includeStop && eCompare<=0)||(!includeStop && eCompare<0)){
                if(frequencies==null)
                    frequencies = new TreeSet<FrequencyEstimate<T>>(freqComparator);
                frequencies.add(f);
            }
        }

        if(frequencies==null) return Collections.emptySet();
        else return frequencies;
    }

    private class ZeroFreq implements FrequencyEstimate<T> {
        private final T item;

        public ZeroFreq(T item) {
            this.item = item;
        }

        @Override
        public T getValue() {
            return item;
        }

        @Override public long count() { return 0; }
        @Override public long error() { return 0; }
    }
}
