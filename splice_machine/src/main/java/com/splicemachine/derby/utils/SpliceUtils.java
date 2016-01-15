package com.splicemachine.derby.utils;

import com.splicemachine.uuid.UUIDService;
import org.apache.log4j.Logger;

/**
 *
 * Splice Utility Methods
 *
 */

public class SpliceUtils {
    private static Logger LOG = Logger.getLogger(SpliceUtils.class);

    public static byte[] getUniqueKey(){
        /*
         * We have to use a UUIDService here, because the data dictionary may want to
         * make use of this method before the EngineDriver has been fully populated; however, most
         * lifecycle services will set the UUID generator beforehand, so it'll still be the right
         * generator, just a different access path.
         */
        return UUIDService.newUuidGenerator(1).nextBytes();
    }

}
