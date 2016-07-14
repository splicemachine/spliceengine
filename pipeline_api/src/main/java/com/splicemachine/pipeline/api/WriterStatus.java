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
