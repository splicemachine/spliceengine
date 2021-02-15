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

package com.splicemachine.pipeline.client;

import com.splicemachine.pipeline.api.WriteStats;

/**
 * @author Scott Fines
 *         Date: 2/5/14
 */
public class SimpleWriteStats implements WriteStats {
    private long writtenCounter;
    private long retryCounter;
    private long thrownErrorsRows;
    private long retriedRows;
    private long partialRows;
    private long partialThrownErrorRows;
    private long partialRetriedRows;
    private long partialIgnoredRows;
    private long partialWrite;
    private long ignoredRows;
    private long catchThrownRows;
    private long catchRetriedRows;
    private long regionTooBusy;
    private Exception e;

    public SimpleWriteStats(long writtenCounter, long retryCounter, long thrownErrorsRows, long retriedRows,
                            long partialRows, long partialThrownErrorRows, long partialRetriedRows,
                            long partialIgnoredRows, long partialWrite, long ignoredRows, long catchThrownRows,
                            long catchRetriedRows, long regionTooBusy) {
        this.writtenCounter = writtenCounter;
        this.retryCounter = retryCounter;
        this.thrownErrorsRows = thrownErrorsRows;
        this.retriedRows = retriedRows;
        this.partialRows = partialRows;
        this.partialThrownErrorRows = partialThrownErrorRows;
        this.partialRetriedRows = partialRetriedRows;
        this.partialIgnoredRows = partialIgnoredRows;
        this.partialWrite = partialWrite;
        this.ignoredRows = ignoredRows;
        this.catchThrownRows = catchThrownRows;
        this.catchRetriedRows = catchRetriedRows;
        this.regionTooBusy = regionTooBusy;
    }
    public SimpleWriteStats(Exception e) {
        this.e = e;
    }

    @Override
    public Exception getException() { return e; }

    @Override
    public long getWrittenCounter() {
        return writtenCounter;
    }

    @Override
    public long getRetryCounter() {
        return retryCounter;
    }

    @Override
    public long getThrownErrorsRows() {
        return thrownErrorsRows;
    }

    @Override
    public long getRetriedRows() {
        return retriedRows;
    }

    @Override
    public long getPartialRows() {
        return partialRows;
    }

    @Override
    public long getPartialThrownErrorRows() {
        return partialThrownErrorRows;
    }

    @Override
    public long getPartialRetriedRows() {
        return partialRetriedRows;
    }

    @Override
    public long getPartialIgnoredRows() {
        return partialIgnoredRows;
    }

    @Override
    public long getPartialWrite() {
        return partialWrite;
    }

    @Override
    public long getIgnoredRows() {
        return ignoredRows;
    }

    @Override
    public long getCatchThrownRows() {
        return catchThrownRows;
    }

    @Override
    public long getCatchRetriedRows() {
        return catchRetriedRows;
    }

    @Override
    public long getRegionTooBusy() {
        return regionTooBusy;
    }

}
