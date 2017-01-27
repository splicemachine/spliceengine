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
