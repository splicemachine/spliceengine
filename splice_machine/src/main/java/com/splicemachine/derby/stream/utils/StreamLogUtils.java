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

package com.splicemachine.derby.stream.utils;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

/**
 *
 * Utils for logging a locatedRow
 *
 */
public class StreamLogUtils {
    private static Logger LOG = Logger.getLogger(StreamLogUtils.class);

    public static void logOperationRecord(LocatedRow locatedRow, OperationContext operationContext) {
        if (LOG.isTraceEnabled()) {
            SpliceOperation op = operationContext.getOperation();
            SpliceLogUtils.trace(LOG, "%s (%d) -> %s", op.getName(),op.resultSetNumber(), locatedRow);
        }
    }

    public static void logOperationRecord(LocatedRow locatedRow, SpliceOperation operation) {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "%s (%d) -> %s", operation.getName(),operation.resultSetNumber(), locatedRow);
        }
    }


    public static void logOperationRecordWithMessage(LocatedRow locatedRow, OperationContext operationContext, String message) {
        if (LOG.isTraceEnabled()) {
            SpliceOperation op = operationContext.getOperation();
            SpliceLogUtils.trace(LOG, "%s (%d) [%s] -> %s", op.getName(),op.resultSetNumber(), message, locatedRow);
        }
    }

}
