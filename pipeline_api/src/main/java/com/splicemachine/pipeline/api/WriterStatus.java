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

package com.splicemachine.pipeline.api;

import javax.management.MXBean;

/**
 * Status MBean for managing Table writer information.
 *
 * @author Scott Fines
 * Created on: 3/19/13
 */
@MXBean
public interface WriterStatus {

    int getExecutingBufferFlushes();

    long getTotalSubmittedFlushes();

    long getFailedBufferFlushes();

    long getNotServingRegionFlushes();

    long getWrongRegionFlushes();

    long getTimedOutFlushes();

    long getGlobalErrors();

    long getPartialFailures();

    long getMaxFlushTime();

    long getMinFlushTime();

    long getTotalFlushTime();

    long getMaxFlushedBufferSize();

    long getTotalFlushedBufferSize();

    double getAvgFlushedBufferSize();

    long getMinFlushedBufferSize();

    double getAvgFlushTime();

    long getMaxFlushedBufferEntries();

    long getTotalFlushedBufferEntries();

    double getAvgFlushedBufferEntries();

    long getMinFlushedBufferEntries();

    void reset();

	long getTotalRejectedFlushes();

    long getMaxRegionsPerFlush();

    long getMinRegionsPerFlush();

    long getAvgRegionsPerFlush();

    double getOverallWriteThroughput();

    double getAvgFlushedEntriesPerRegion();

    double getAvgFlushedSizePerRegion();
}
