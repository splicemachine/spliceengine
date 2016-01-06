package com.splicemachine.derby.utils;

import com.splicemachine.EngineDriver;
import com.splicemachine.primitives.Bytes;

/**
 * @author Scott Fines
 * Date: 7/29/14
 */
public class UUIDFunction {

    /**
     * @return a UUID generated as an 8-byte UUID block.
     */
    public static long NEXT_UUID(){
        //TODO -sf- find a way to do this without synchronizing on every call
        //maybe make a "RandomSequence" construct?
        byte[] bytes=EngineDriver.driver().newUUIDGenerator(1).nextBytes();
        return Bytes.toLong(bytes,0,8); //take the first 8 bytes, in case it's longer
    }
}
