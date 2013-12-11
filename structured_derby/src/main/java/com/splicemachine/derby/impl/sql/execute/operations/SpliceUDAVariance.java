package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.agg.Aggregator;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.error.StandardException;

public class SpliceUDAVariance<K extends Double>
        implements Aggregator<K,K,SpliceUDAVariance<K>>
{
    int count;
    double sum;
    double variance;

    public SpliceUDAVariance() {

    }

    public void init() {
        count = 0;
        sum = 0;
        variance = 0;
    }

    public void accumulate( K value ) {
        count++;
        double v = value.doubleValue();
        sum += v;
        if (count > 1) {
            double t = count*v - sum;
            variance += (t*t) / ((double)count*(count-1));
        }
    }

    public void merge( SpliceUDAVariance<K> other ) {
        long n = count;
        long m = other.count;

        if (n == 0) {
            variance = other.variance;
            count = other.count;
            sum = other.sum;
        }

        if (m != 0 && n != 0) {
            // Merge the two partials
            double a = sum;
            double b = other.sum;
            count += other.count;
            sum += other.sum;
            double t = (m/(double)n)*a - b;
            variance += other.variance + ((n/(double)m)/((double)n+m)) * t * t;
        }
    }

    public K terminate() {
        Double r = new Double(variance);
        return (K) r;
    }

    public void add (DataValueDescriptor addend) throws StandardException{
        variance = addend.getDouble();
    }

}

