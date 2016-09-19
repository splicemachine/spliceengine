/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.agg.Aggregator;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.error.StandardException;

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

