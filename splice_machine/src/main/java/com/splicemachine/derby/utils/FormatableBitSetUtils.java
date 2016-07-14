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

import com.splicemachine.db.iapi.services.io.FormatableBitSet;

import java.util.Arrays;

public class FormatableBitSetUtils {
	public static int currentRowPositionFromBaseRow(FormatableBitSet accessedCols, int baseRowPosition) {
		if (accessedCols == null)
			return baseRowPosition;
		int position = 0;
		for (int i = 0; i<baseRowPosition;i++) {
			if (accessedCols.isSet(i))
					position++;
		}
		return position;
	}

    public static int currentRowPositionFromBaseRow(int[] accessedColMap, int baseRowPosition) {
        return accessedColMap[baseRowPosition];
    }

		public static int[] toIntArray(FormatableBitSet validColumns) {
				if(validColumns==null) return null;
				int[] destArray = new int[validColumns.getLength()];
				Arrays.fill(destArray, -1);
				for(int i=validColumns.anySetBit();i>=0;i=validColumns.anySetBit(i)){
						destArray[i] = i;
				}
				return destArray;
		}

	public static int[] toCompactedIntArray(FormatableBitSet validColumns) {
		if(validColumns == null) return null;
		int[] result = new int[validColumns.getNumBitsSet()];
		int count = 0;
		for (int i = validColumns.anySetBit(); i >= 0; i = validColumns.anySetBit(i)) {
			result[count] = i;
			count++;
		}
		return result;
	}

    public static FormatableBitSet fromIntArray(int bitSetSize, int[] columns){
        FormatableBitSet bitSet = new FormatableBitSet(bitSetSize);
        for (int baseColumnPosition : columns) {
            bitSet.set(baseColumnPosition);
        }
        return bitSet;
    }
}
