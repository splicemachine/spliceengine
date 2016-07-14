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
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

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
}

