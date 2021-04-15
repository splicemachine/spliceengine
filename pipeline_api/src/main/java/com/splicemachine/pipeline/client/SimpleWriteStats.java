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
    private final long writtenCounter;
    private final long retryCounter;
    private final long thrownErrorsRows;
    private final long retriedRows;
    private final long partialRows;
    private final long partialThrownErrorRows;
    private final long partialRetriedRows;
    private final long partialIgnoredRows;
    private final long partialWrite;
    private final long ignoredRows;
    private final long catchThrownRows;
    private final long catchRetriedRows;
    private final long regionTooBusy;

    public SimpleWriteStats(long writtenCounter, long retryCounter, long thrownErrorsRows, long retriedRows, long partialRows, long partialThrownErrorRows, long partialRetriedRows, long partialIgnoredRows, long partialWrite, long ignoredRows, long catchThrownRows, long catchRetriedRows, long regionTooBusy) {
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
