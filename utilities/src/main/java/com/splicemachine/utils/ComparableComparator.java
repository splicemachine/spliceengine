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

package com.splicemachine.utils;

import java.util.Comparator;

/**
 * @author Scott Fines
 *         Date: 8/5/14
 */
public class ComparableComparator<E extends Comparable<E>> implements Comparator<E>{
    private static final ComparableComparator INSTANCE = new ComparableComparator();

    @SuppressWarnings("unchecked")
    public static <E extends Comparable<E>>Comparator<? super E> newComparator(){
        return (Comparator<E>)INSTANCE;
    }

    public int compare(E o1, E o2) {
        if(o1==null){
            if(o2==null) return 0;
            return -1;
        }else if(o2==null) return 1;
        else
            return o1.compareTo(o2);
    }
}
