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

package com.splicemachine.stats.estimate;

/**
 * A distribution which contains no records. This is primarily useful when
 * a given partition <em>knows</em> that there is no data which matches a given column--e.g.
 * when statistics were not collected for a specific column.
 *
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class EmptyDistribution<T> implements Distribution<T>{
    private static final EmptyDistribution INSTANCE = new EmptyDistribution();

    @SuppressWarnings("unchecked")
    public static <T> Distribution<T> emptyDistribution(){
        //unchecked cast is fine here, because we don't do anything with the type info anyway
        return (EmptyDistribution<T>)INSTANCE;
    }

    @Override public long selectivity(T element) { return 0; }
    @Override public long rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop) { return 0; }
    @Override public T minValue(){ return null; }
    @Override public long minCount(){ return 0; }
    @Override public T maxValue(){ return null; }
    @Override public long totalCount(){ return 0; }
}
