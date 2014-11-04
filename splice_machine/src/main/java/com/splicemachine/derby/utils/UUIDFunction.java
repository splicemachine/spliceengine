package com.splicemachine.derby.utils;

import com.splicemachine.derby.hbase.SpliceDriver;

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
        return SpliceDriver.driver().getUUIDGenerator().nextUUID();
    }
}
