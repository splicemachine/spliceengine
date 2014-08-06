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
        return o1.compareTo(o2);
    }
}
