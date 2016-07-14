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

package com.splicemachine.pipeline.traffic;

public class WriteStatus {
    int dependentWriteThreads;
    int independentWriteThreads;
    int dependentWriteCount;
    int independentWriteCount;

    public WriteStatus(int dependentWriteThreads, int dependentWriteCount,
                       int independentWriteCount, int independentWriteThreads) {
        assert (dependentWriteThreads >= 0 &&
                independentWriteThreads >= 0 &&
                dependentWriteCount >= 0 &&
                independentWriteCount >= 0);
        this.dependentWriteThreads = dependentWriteThreads;
        this.independentWriteThreads = independentWriteThreads;
        this.dependentWriteCount = dependentWriteCount;
        this.independentWriteCount = independentWriteCount;
    }


    public static WriteStatus incrementDependentWriteStatus(WriteStatus clone, int writes) {
        return new WriteStatus(clone.dependentWriteThreads + 1, clone.dependentWriteCount + writes,
                clone.independentWriteCount, clone.independentWriteThreads);
    }

    public static WriteStatus incrementIndependentWriteStatus(WriteStatus clone, int writes) {
        return new WriteStatus(clone.dependentWriteThreads, clone.dependentWriteCount,
                clone.independentWriteCount + writes, clone.independentWriteThreads + 1);
    }

    public static WriteStatus decrementDependentWriteStatus(WriteStatus clone, int writes) {
        return new WriteStatus(clone.dependentWriteThreads - 1, clone.dependentWriteCount - writes,
                clone.independentWriteCount, clone.independentWriteThreads);
    }

    public static WriteStatus decrementIndependentWriteStatus(WriteStatus clone, int writes) {
        return new WriteStatus(clone.dependentWriteThreads, clone.dependentWriteCount,
                clone.independentWriteCount - writes, clone.independentWriteThreads - 1);
    }

    @Override
    public String toString() {
        return String.format("{ dependentWriteThreads=%d, independentWriteThreads=%d, "
                        + "dependentWriteCount=%d, independentWriteCount=%d }", dependentWriteThreads, independentWriteThreads,
                dependentWriteCount, independentWriteCount);
    }

    public int getDependentWriteThreads() {
        return dependentWriteThreads;
    }

    public int getIndependentWriteThreads() {
        return independentWriteThreads;
    }

    public int getDependentWriteCount() {
        return dependentWriteCount;
    }

    public int getIndependentWriteCount() {
        return independentWriteCount;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || (obj instanceof WriteStatus) &&
                this.dependentWriteCount == ((WriteStatus) obj).dependentWriteCount &&
                this.dependentWriteThreads == ((WriteStatus) obj).dependentWriteThreads &&
                this.independentWriteCount == ((WriteStatus) obj).independentWriteCount &&
                this.independentWriteThreads == ((WriteStatus) obj).independentWriteThreads;
    }
    @Override
    public int hashCode(){
        int hC = 17;
        hC+= 31*hC+dependentWriteCount;
        hC+= 31*hC+dependentWriteThreads;
        hC+= 31*hC+independentWriteCount;
        hC+= 31*hC+independentWriteThreads;
        return hC;
    }
}