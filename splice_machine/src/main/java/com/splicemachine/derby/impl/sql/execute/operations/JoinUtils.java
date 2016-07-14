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
        } else {
            System.arraycopy(leftRowArray, 0, mergedRowArray, 0, leftRowArray.length);
            System.arraycopy(rightRowArray, 0, mergedRowArray, leftRowArray.length, rightRowArray.length);
        }
        return mergedRow;
    }


}
