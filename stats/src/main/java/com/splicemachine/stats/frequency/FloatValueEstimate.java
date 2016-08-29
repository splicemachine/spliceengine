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
import org.spark_project.guava.primitives.Floats;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
class FloatValueEstimate implements FloatFrequencyEstimate {
    private float value;
    private long count;
    private long epsilon;

    public FloatValueEstimate(float v, long c, long eps) {
        this.value = v;
        this.count = c;
        this.epsilon = eps;
    }

    @Override public float value() { return value; }

    @Override
    public int compareTo(FloatFrequencyEstimate o) {
        int compare = Floats.compare(value, o.value());
        if(compare!=0) return compare;
        compare = Longs.compare(count, o.count());
        if(compare!=0) return compare;
        return Longs.compare(epsilon,o.error());
    }

    @Override public Float getValue() { return value; }
    @Override public long count() { return count; }
    @Override public long error() { return epsilon; }

    @Override
    public FrequencyEstimate<Float> merge(FrequencyEstimate<Float> other) {
        this.count+=other.count();
        this.epsilon+=other.error();
        return this;
    }

    @Override
    public int hashCode() {
        return Floats.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof FrequencyEstimate)) return false;
        if(obj instanceof FloatFrequencyEstimate)
            return ((FloatFrequencyEstimate)obj).value()==value;
        Object ob = ((FrequencyEstimate)obj).getValue();
        return ob instanceof Float && (Float)ob == value;
    }

    @Override
    public String toString() {
        return "("+value+","+count+","+epsilon+")";
    }
}
