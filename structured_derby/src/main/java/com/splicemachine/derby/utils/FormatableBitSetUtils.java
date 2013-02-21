package com.splicemachine.derby.utils;

import org.apache.derby.iapi.services.io.FormatableBitSet;

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
}
