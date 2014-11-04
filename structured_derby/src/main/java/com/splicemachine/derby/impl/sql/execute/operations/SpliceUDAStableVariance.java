package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.agg.Aggregator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * Numerically stable variance computation, taken from
 * <a href="http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm">http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm</a>
 * which in turn takes it from Donald E. Knuth (1998). The Art of Computer Programming, volume 2: Seminumerical Algorithms, 3rd edn., p. 232. Boston: Addison-Wesley.
 */
public class SpliceUDAStableVariance<K extends Double>
        implements Aggregator<K,K,SpliceUDAStableVariance<K>>
{
    int count;
    double m2;
    double mean;

    public SpliceUDAStableVariance() {
    }

    public void init() {
        count = 0;
        m2 = 0;
        mean = 0;
    }

    public void accumulate( K value ) {
        count++;
        double x = value.doubleValue();
        double delta = x - mean;
        mean = mean + delta/count;
        m2 = m2 + delta*(x - mean);
    }

    public void merge( SpliceUDAStableVariance<K> other ) {
        throw new UnsupportedOperationException();
    }

    public K terminate() {
        return (K) new Double(m2 / (count - 1));
    }

    @Override
    public void add(DataValueDescriptor addend) throws StandardException {
        throw new UnsupportedOperationException();
    }

}

