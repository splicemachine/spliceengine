/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
