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
