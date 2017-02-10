/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
