package com.splicemachine.derby.utils;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.util.List;

/**
 *
 * Splice Utility Methods
 *
 */

public class SpliceUtils {
    private static Logger LOG = Logger.getLogger(SpliceUtils.class);

    public static byte[] getUniqueKey(){
        return EngineDriver.driver().newUUIDGenerator(1).nextBytes();
    }

}
