package com.splicemachine.derby.utils;

/**
 * Generic Utilities relating to Derby stuff
 *
 * @author Scott Fines
 * Created on: 3/7/13
 */
public class DerbyUtils {

    private DerbyUtils(){}

    /**
     * Translates a one-based integer map to a zero-based.
     *
     * Derby uses one-based indexing, whereas we tend to want zero-based.
     * This will convert the int array from one to the other.
     *
     * @param oneBasedMap the map to translate
     * @return
     */
    public static int[] translate(int[] oneBasedMap){
        int[] zeroBased = new int[oneBasedMap.length-1];
        for(int zeroPos=0,onePos=1;onePos<oneBasedMap.length;zeroPos++,onePos++){
            zeroBased[zeroPos] = oneBasedMap[onePos];
        }
        return zeroBased;
    }

    public static void translateInPlace(int[] oneBasedMap){
        for(int zeroPos=0,onePos=1;onePos<oneBasedMap.length;zeroPos++,onePos++){
            oneBasedMap[zeroPos] = oneBasedMap[onePos];
        }
        oneBasedMap[oneBasedMap.length-1] = -1;
    }
}
