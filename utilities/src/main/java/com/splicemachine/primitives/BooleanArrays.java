package com.splicemachine.primitives;

/**
 * Utility class for dealing with boolean arrays.
 *
 * @author Scott Fines
 * Date: 8/26/14
 */
public class BooleanArrays {

    private BooleanArrays(){} //can't instantiate me!

    public static boolean[] not(boolean[] array){
        boolean[] not = new boolean[array.length];
        for(int i=0;i<array.length;i++){
            not[i] = !array[i];
        }
        return not;
    }
}
