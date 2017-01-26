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

package com.splicemachine.pipeline.client;

import com.splicemachine.metrics.*;
import com.splicemachine.pipeline.api.WriteStats;

/**
 * @author Scott Fines
 *         Date: 2/5/14
 */
public class MergingWriteStats implements WriteStats {
    private final Counter writtenCounter;
    private final Counter retryCounter;
    private final Counter thrownErrorsRows;
    private final Counter retriedRows;
    private final Counter partialRows;
    private final Counter partialThrownErrorRows;
    private final Counter partialRetriedRows;
    private final Counter partialIgnoredRows;
    private final Counter partialWrite;
    private final Counter ignoredRows;
    private final Counter catchThrownRows;
    private final Counter catchRetriedRows;
    private final Counter regionTooBusy;

    public MergingWriteStats(MetricFactory metricFactory) {
        this.writtenCounter = metricFactory.newCounter();
        this.retryCounter = metricFactory.newCounter();
        this.thrownErrorsRows = metricFactory.newCounter();
        this.retriedRows = metricFactory.newCounter();
        this.partialRows = metricFactory.newCounter();
        this.partialThrownErrorRows = metricFactory.newCounter();
        this.partialRetriedRows = metricFactory.newCounter();
        this.partialIgnoredRows = metricFactory.newCounter();
        this.partialWrite = metricFactory.newCounter();
        this.ignoredRows = metricFactory.newCounter();
        this.catchThrownRows = metricFactory.newCounter();
        this.catchRetriedRows = metricFactory.newCounter();
        this.regionTooBusy = metricFactory.newCounter();
    }

    public void merge(WriteStats newStats) {
        writtenCounter.add(newStats.getWrittenCounter());
        retryCounter.add(newStats.getRetryCounter());
        thrownErrorsRows.add(newStats.getThrownErrorsRows());
        retriedRows.add(newStats.getRetriedRows());
        partialRows.add(newStats.getPartialRows());
        partialThrownErrorRows.add(newStats.getPartialThrownErrorRows());
        partialRetriedRows.add(newStats.getPartialRetriedRows());
        partialIgnoredRows.add(newStats.getPartialIgnoredRows());
        partialWrite.add(newStats.getPartialWrite());
        ignoredRows.add(newStats.getIgnoredRows());
        catchThrownRows.add(newStats.getCatchThrownRows());
        catchRetriedRows.add(newStats.getCatchRetriedRows());
        regionTooBusy.add(newStats.getRegionTooBusy());
    }

    @Override
    public long getWrittenCounter() {
        return writtenCounter.getTotal();
    }

    @Override
    public long getRetryCounter() {
        return retryCounter.getTotal();
    }

    @Override
    public long getThrownErrorsRows() {
        return thrownErrorsRows.getTotal();
    }

    @Override
    public long getRetriedRows() {
        return retriedRows.getTotal();
    }

    @Override
    public long getPartialRows() {
        return partialRows.getTotal();
    }

    @Override
    public long getPartialThrownErrorRows() {
        return partialThrownErrorRows.getTotal();
    }

    @Override
    public long getPartialRetriedRows() {
        return partialRetriedRows.getTotal();
    }

    @Override
    public long getPartialIgnoredRows() {
        return partialIgnoredRows.getTotal();
    }

    @Override
    public long getPartialWrite() {
        return partialWrite.getTotal();
    }

    @Override
    public long getIgnoredRows() {
        return ignoredRows.getTotal();
    }

    @Override
    public long getCatchThrownRows() {
        return catchThrownRows.getTotal();
    }

    @Override
    public long getCatchRetriedRows() {
        return catchRetriedRows.getTotal();
    }

    @Override
    public long getRegionTooBusy() {
        return regionTooBusy.getTotal();
    }
}
