/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
