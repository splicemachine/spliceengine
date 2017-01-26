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
