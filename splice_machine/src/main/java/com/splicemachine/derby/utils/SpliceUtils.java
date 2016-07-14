/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
