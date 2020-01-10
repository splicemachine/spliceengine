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
 *
 */

package com.splicemachine.db.iapi.sql.conn;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by dgomezferro on 5/26/17.
 */
public class ControlExecutionLimiterImpl implements ControlExecutionLimiter {
    private final long rowsLimit;
    private final AtomicLong currentRows;

    public ControlExecutionLimiterImpl(long limit) {
        this.rowsLimit = limit;
        this.currentRows = new AtomicLong();
    }

    public void addAccumulatedRows(long rows) {
        if (rowsLimit > 0 && currentRows.addAndGet(rows) > rowsLimit) {
            doThrow(new ResubmitDistributedException());
        }
    }

    @SuppressWarnings("unchecked")
    static <T extends Throwable> void doThrow(Throwable t) throws T {
        throw (T) t;
    }
}
