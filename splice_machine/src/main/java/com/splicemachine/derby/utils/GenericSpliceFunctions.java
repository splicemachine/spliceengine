package com.splicemachine.derby.utils;

import com.splicemachine.EngineDriver;
import com.splicemachine.primitives.Bytes;

/**
 * @author Scott Fines
 *         Date: 7/31/14
 */
public class GenericSpliceFunctions {

    private GenericSpliceFunctions(){} //don't instantiate utility class

    public static long LONG_UUID(){
        return Bytes.toLong(EngineDriver.driver().newUUIDGenerator(1).nextBytes());
    }
}
