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
