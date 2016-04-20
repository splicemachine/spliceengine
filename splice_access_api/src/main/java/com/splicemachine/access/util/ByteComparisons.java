package com.splicemachine.access.util;

import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;

/**
 * @author Scott Fines
 *         Date: 4/20/16
 */
public class ByteComparisons{
    private static volatile ByteComparator COMPARATOR = Bytes.basicByteComparator();

    private ByteComparisons(){}

    public static ByteComparator comparator(){
        return COMPARATOR;
    }

    public static void setComparator(ByteComparator comparator){
        COMPARATOR = comparator;
    }
}
