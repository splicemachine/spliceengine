package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

public class JoinUtils {
    private static Logger LOG = Logger.getLogger(JoinUtils.class);

    public static enum JoinSide {RIGHT, LEFT} ;

    public static ExecRow getMergedRow(ExecRow leftRow, ExecRow rightRow,
                                       boolean wasRightOuterJoin, ExecRow mergedRow) {
        if (mergedRow == null) {
            return null;
        }
        DataValueDescriptor[] leftRowArray = leftRow.getRowArray();
        DataValueDescriptor[] rightRowArray = rightRow.getRowArray();
        @SuppressWarnings("MismatchedReadAndWriteOfArray") DataValueDescriptor[] mergedRowArray = mergedRow.getRowArray();
        if (wasRightOuterJoin) {
            System.arraycopy(rightRowArray, 0, mergedRowArray, 0, rightRowArray.length);
            System.arraycopy(leftRowArray, 0, mergedRowArray, rightRowArray.length, leftRowArray.length);
        } else {
            System.arraycopy(leftRowArray, 0, mergedRowArray, 0, leftRowArray.length);
            System.arraycopy(rightRowArray, 0, mergedRowArray, leftRowArray.length, rightRowArray.length);
        }
        return mergedRow;
    }


}
