package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.agg.Aggregator;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.error.StandardException;


/**
 * Evaluate the variance using the algorithm described by Chan, Golub, and LeVeque in
 * "Algorithms for computing the sample variance: analysis and recommendations"
 * The American Statistician, 37 (1983) pp. 242--247.
 *
 * variance = variance1 + variance2 + n/(m*(m+n)) * pow(((m/n)*t1 - t2),2)
 *
 * where: - variance is sum[x-avg^2] (this is actually n times the variance)
 * and is updated at every step. - n is the count of elements in chunk1 - m is
 * the count of elements in chunk2 - t1 = sum of elements in chunk1, t2 =
 * sum of elements in chunk2.
 *
 * This algorithm was proven to be numerically stable by J.L. Barlow in
 * "Error analysis of a pairwise summation algorithm to compute sample variance"
 * Numer. Math, 58 (1991) pp. 583--590
 *
 */
public class SpliceUDAVariance<K extends Double> implements Aggregator<K,K,SpliceUDAVariance<K>> {
    long count;
    double variance;
    double mean;

    public SpliceUDAVariance() {

    }

    public void init() {
        count = 0;
        variance = 0;
        mean = 0;
    }

    public void accumulate( K value ) {
        count++;
        double x = value.doubleValue();
        double delta = x - mean;
        mean = mean + delta/count;
        variance = variance + delta*(x - mean);
    }

    public void merge( SpliceUDAVariance<K> other ) {
        long n = count;
        long m = other.count;

        if (n == 0) {
            variance = other.variance;
            count = other.count;
            mean = other.mean;
        }

        if (m != 0 && n != 0) {
            // Merge the two partials
            double a = mean * n;
            double b = other.mean * m;
            count += other.count;
            mean = (a + b) / count;
            double t = (m / (double) n) * a - b;
            variance += other.variance + ((n / (double)m) / ((double)n + m)) * t * t;
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

