/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.agg.Aggregator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


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
public class SpliceUDAVariance<K extends Double> implements Aggregator<K,K,SpliceUDAVariance<K>>, Externalizable {
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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(count);
        out.writeDouble(variance);
        out.writeDouble(mean);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        count = in.readLong();
        variance = in.readDouble();
        mean = in.readDouble();
    }
}

