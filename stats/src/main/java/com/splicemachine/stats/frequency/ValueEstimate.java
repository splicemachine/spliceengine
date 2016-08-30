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

package com.splicemachine.stats.frequency;

import org.spark_project.guava.primitives.Longs;

import java.util.Comparator;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
class ValueEstimate<T> implements FrequencyEstimate<T>,Comparable<FrequencyEstimate<T>> {
    private T value;
    private long count;
    private long epsilon;
    private Comparator<? super T> comparator;

    public ValueEstimate(T v, long c, long eps, Comparator<? super T> comparator) {
        this.value = v;
        this.count = c;
        this.epsilon = eps;
        this.comparator = comparator;
    }

    @Override public T getValue() { return value; }

    @Override
    public int compareTo(FrequencyEstimate<T> o) {
        int compare = comparator.compare(value, o.getValue());
        if(compare!=0) return compare;
        compare = Longs.compare(count, o.count());
        if(compare!=0) return compare;
        return Longs.compare(epsilon,o.error());
    }

    @Override public long count() { return count; }
    @Override public long error() { return epsilon; }

    @Override
    public FrequencyEstimate<T> merge(FrequencyEstimate<T> other) {
        this.count+=other.count();
        this.epsilon+=other.error();
        return this;
    }

    @Override
    public String toString() {
        return "("+value+","+count+","+epsilon+")";
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof FrequencyEstimate)) return false;
        FrequencyEstimate oEst = (FrequencyEstimate)obj;
        return value.equals(oEst.getValue());
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
