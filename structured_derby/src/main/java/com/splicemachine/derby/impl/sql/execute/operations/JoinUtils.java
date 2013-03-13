package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class JoinUtils {
	private static Logger LOG = Logger.getLogger(JoinUtils.class);
	public static enum JoinSide {RIGHT,LEFT};
	public static final byte[] JOIN_SIDE_COLUMN = (new Integer(-1)).toString().getBytes();
	
	public static ExecRow getMergedRow(ExecRow leftRow, ExecRow rightRow, boolean wasRightOuterJoin,int rightNumCols, int leftNumCols, ExecRow mergedRow) {
//		SpliceLogUtils.trace(LOG, "getMergedRow with leftRow %s,right row %s, rightOuterJoin?%b" , leftRow , rightRow,wasRightOuterJoin);
		int colInCtr;
		int colOutCtr;
		/* Reverse left and right for return of row if this was originally
		 * a right outer join.  (Result columns ordered according to
		 * original query.)
		 */
		if (wasRightOuterJoin) {
			ExecRow tmp;
			tmp = leftRow;
			leftRow = rightRow;
			rightRow = tmp;
			leftNumCols = rightNumCols;
			rightNumCols = leftNumCols;
		} 

		/* Merge the rows, doing just in time allocation for mergedRow.
		 * (By convention, left Row is to left of right Row.)
		 */
		try {
			for (colInCtr = 1, colOutCtr = 1; colInCtr <= leftNumCols;colInCtr++, colOutCtr++) {
//				SpliceLogUtils.trace(LOG,"colInCtr=%d,colOutCtr=%d",colInCtr,colOutCtr);
				DataValueDescriptor src_col = leftRow.getColumn(colInCtr);
				// Clone the value if it is represented by a stream (DERBY-3650).
				if (src_col != null && src_col.hasStream()) {
					src_col = src_col.cloneValue(false);
				}
				mergedRow.setColumn(colOutCtr, src_col);
			}
//			SpliceLogUtils.trace(LOG,"colOutCtr=%d",colOutCtr);
			for (colInCtr = 1; colInCtr <= rightNumCols;colInCtr++, colOutCtr++) {
				DataValueDescriptor src_col = rightRow.getColumn(colInCtr);
				// Clone the value if it is represented by a stream (DERBY-3650).
				if (src_col != null && src_col.hasStream()) {
					src_col = src_col.cloneValue(false);
				}
				mergedRow.setColumn(colOutCtr, src_col);
			}
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Error merging rows", e);
		}
        SpliceLogUtils.trace(LOG, "final mergedRow " + mergedRow);
		return mergedRow;
	}

	
}
