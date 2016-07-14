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

/**
 * Generic Utilities relating to Derby stuff
 *
 * @author Scott Fines
 * Created on: 3/7/13
 */
public class DerbyUtils {

    private DerbyUtils(){}

    /**
     * Translates a one-based integer map to a zero-based.
     *
     * Derby uses one-based indexing, whereas we tend to want zero-based.
     * This will convert the int array from one to the other.
     *
     * @param oneBasedMap the map to translate
     * @return
     */
    public static int[] translate(int[] oneBasedMap){
        int[] zeroBased = new int[oneBasedMap.length-1];
        for(int zeroPos=0,onePos=1;onePos<oneBasedMap.length;zeroPos++,onePos++){
            zeroBased[zeroPos] = oneBasedMap[onePos];
        }
        return zeroBased;
    }

    public static void translateInPlace(int[] oneBasedMap){
        for(int zeroPos=0,onePos=1;onePos<oneBasedMap.length;zeroPos++,onePos++){
            oneBasedMap[zeroPos] = oneBasedMap[onePos];
        }
        oneBasedMap[oneBasedMap.length-1] = -1;
    }
}
