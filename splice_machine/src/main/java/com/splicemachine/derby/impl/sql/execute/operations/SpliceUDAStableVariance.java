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

