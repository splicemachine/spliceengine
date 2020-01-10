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
