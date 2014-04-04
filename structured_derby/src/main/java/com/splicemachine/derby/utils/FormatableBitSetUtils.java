package com.splicemachine.derby.utils;

import org.apache.derby.iapi.services.io.FormatableBitSet;

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
}
