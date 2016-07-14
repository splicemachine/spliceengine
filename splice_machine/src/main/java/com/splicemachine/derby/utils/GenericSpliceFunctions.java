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
