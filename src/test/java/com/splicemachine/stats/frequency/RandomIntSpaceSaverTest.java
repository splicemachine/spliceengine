package com.splicemachine.stats.frequency;

import com.carrotsearch.hppc.IntLongOpenHashMap;
import com.carrotsearch.hppc.cursors.IntLongCursor;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.splicemachine.stats.random.Distributions;
import com.splicemachine.stats.random.ParetoDistribution;
import com.splicemachine.stats.random.RandomDistribution;
import org.junit.Test;

import java.util.*;

/**
 * Uses a random distribution of data to test the int frequent elements.
 *
 * @author Scott Fines
 *         Date: 2/20/15
 */
public class RandomIntSpaceSaverTest {

    private int numIterations =10000;

    @Test
    public void testParetoDistributionTopK() throws Exception {
        RandomDistribution distribution = new ParetoDistribution(Distributions.uniform(),1.0,1.0);
        IntLongOpenHashMap actualDistribution = new IntLongOpenHashMap();
        IntFrequencyCounter counter = FrequencyCounters.intCounter(100);

        addData(distribution, actualDistribution, counter);

        //now find the k most frequent items
        int k = 100;

        Set<IntFrequencyEstimate> actualFreqElements = getActualTopK(k,actualDistribution);

        IntFrequentElements estFreqElements = counter.frequentElements(k+3);

        //make sure that the estimate contains the actual
        for(IntFrequencyEstimate actualFreqElement:actualFreqElements){
            IntFrequencyEstimate estimate = estFreqElements.countEqual(actualFreqElement.value());
            if(estimate.count()<actualFreqElement.count()){
                System.out.printf("Estimate is smaller than the actual! Actual: %s,Estimate: %s%n",actualFreqElement,estimate);
            }
        }
    }

    @Test
    public void testParetoDistributionHeavyHitters() throws Exception {
        long time = System.currentTimeMillis();
        Random random = new Random(time);
        RandomDistribution distribution = new ParetoDistribution(Distributions.uniform(random),1.0,1.0);
        IntLongOpenHashMap actualDistribution = new IntLongOpenHashMap();
        IntFrequencyCounter counter = FrequencyCounters.intCounter(100);

        addData(distribution, actualDistribution, counter);
        float support =0.1f;

        Set<IntFrequencyEstimate> actualFreqElements = getActualHeavyHitters(support,actualDistribution);

        IntFrequentElements estFreqElements = counter.heavyHitters(support);

        //make sure that the estimate contains the actual
        for(IntFrequencyEstimate actualFreqElement:actualFreqElements){
            IntFrequencyEstimate estimate = estFreqElements.countEqual(actualFreqElement.value());
            if(estimate.count()<actualFreqElement.count()){
                System.out.printf("Estimate is smaller than the actual! Actual: %s,Estimate: %s%n",actualFreqElement,estimate);
            }
        }
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private void addData(RandomDistribution distribution,
                         IntLongOpenHashMap actualDistribution,
                         IntFrequencyCounter counter) {
        for(int i=0;i<numIterations;i++){
           int toAdd = distribution.nextInt();
            actualDistribution.addTo(toAdd,1l);
            counter.update(toAdd);
        }
    }

    private Set<IntFrequencyEstimate> getActualHeavyHitters(float support,IntLongOpenHashMap actualDistribution) {
        IntFrequencyEstimate[] values = sortDistribution(actualDistribution);
        Set<IntFrequencyEstimate> actualFreqElements = new TreeSet<>();
        long supportLevel = (long)(support*numIterations);
        for(IntFrequencyEstimate est:values){
            if(est.count()>=supportLevel)
                actualFreqElements.add(est);
        }
        return actualFreqElements;
    }

    private Set<IntFrequencyEstimate> getActualTopK(int k,IntLongOpenHashMap actualDistribution) {
        IntFrequencyEstimate[] values = sortDistribution(actualDistribution);
        int i;
        Set<IntFrequencyEstimate> actualFreqElements = new TreeSet<>();
        if(k>actualDistribution.size())k = actualDistribution.size();
        for(i=0;i<k;i++){
            actualFreqElements.add(values[i]);
        }
        return actualFreqElements;
    }

    private IntFrequencyEstimate[] sortDistribution(IntLongOpenHashMap actualDistribution) {
        IntFrequencyEstimate[] values = new IntFrequencyEstimate[actualDistribution.size()];
        int i=0;
        for(IntLongCursor cursor:actualDistribution){
            values[i] = new ActualFrequency(cursor);
            i++;
        }

        Arrays.sort(values, new Comparator<IntFrequencyEstimate>() {
            @Override
            public int compare(IntFrequencyEstimate o1, IntFrequencyEstimate o2) {
                int compare = Longs.compare(o1.count(), o2.count());
                if (compare != 0) return -1 * compare;
                return Ints.compare(o1.value(), o2.value()); //sort in ascending order when the same count is used
            }
        });
        return values;
    }

    private class ActualFrequency implements IntFrequencyEstimate {
        int value;
        long count;
        public ActualFrequency(IntLongCursor value) {
            this.value = value.key;
            this.count = value.value;
        }

        @Override
        public int compareTo(IntFrequencyEstimate o) {
            int compare = Ints.compare(value,o.value());
            if(compare!=0) return compare;
            return Longs.compare(count,o.count());
        }

        @Override public int value() { return value; }
        @Override public Integer getValue() { return value; }
        @Override public long count() { return count; }
        @Override public long error() { return 0; }

        @Override
        public String toString() {
            return "("+value+","+count+",0)";
        }
    }
}
