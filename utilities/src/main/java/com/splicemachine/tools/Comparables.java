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

package com.splicemachine.tools;

import java.util.Comparator;

/**
 * Utility class around Comparables and Comparators.
 *
 * @author Scott Fines
 * Date: 10/7/14
 */
public class Comparables {


    private static final Comparator<? extends Comparable> NULLS_FIRST = new Comparator<Comparable>() {
        @SuppressWarnings("unchecked")
        @Override
        public int compare(Comparable o1, Comparable o2) {
            if(o1==null){
                if(o2==null) return 0;
                return -1; //put nulls first
            }else if(o2==null) return 1; //put nulls first
            else return o1.compareTo(o2);
        }
    };

    private static final Comparator<? extends Comparable> NULLS_LAST = new Comparator<Comparable>() {
        @SuppressWarnings("unchecked")
        @Override
        public int compare(Comparable o1, Comparable o2) {
            if(o1==null){
                if(o2==null) return 0;
                return 1; //put nulls last
            }else if(o2==null) return -1; //put nulls last
            else return o1.compareTo(o2);
        }
    };

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<T>> Comparator<T> nullsLastComparator(){
        return (Comparator<T>)NULLS_LAST;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<T>> Comparator<T> nullsFirstComparator(){
        return (Comparator<T>)NULLS_FIRST;
    }
}
