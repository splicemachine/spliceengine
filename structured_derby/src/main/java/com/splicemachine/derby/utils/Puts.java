package com.splicemachine.derby.utils;

/**
 * Utility class for creating HBase puts from Derby-specific data structures.
 *
 * @author Scott Fines
 * @author John Leach
 * @author Jessie Zhang
 * Created: 1/24/13 2:00 PM
 */
public class Puts {
    public static final byte[] FOR_UPDATE = "U".getBytes();
    public static final String PUT_TYPE = "t";

    private Puts(){}


}
