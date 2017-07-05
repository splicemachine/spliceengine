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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

public class JoinUtils {
    private static Logger LOG = Logger.getLogger(JoinUtils.class);

    public enum JoinSide {RIGHT, LEFT}

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
            mergedRow.setKey(rightRow.getKey());
        } else {
            System.arraycopy(leftRowArray, 0, mergedRowArray, 0, leftRowArray.length);
            System.arraycopy(rightRowArray, 0, mergedRowArray, leftRowArray.length, rightRowArray.length);
            mergedRow.setKey(leftRow.getKey());
        }
        return mergedRow;
    }


}
