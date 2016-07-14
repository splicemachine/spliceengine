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

package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.TableStatistics;

/**
 * @author Scott Fines
 *         Date: 3/25/15
 */
public interface OverheadManagedTableStatistics extends TableStatistics{

    /**
     * @return the latency to open a scanner against this table (in microseconds), averaged
     * over all partitions
     */
    double openScannerLatency();

    /**
     * @return the latency to close a scanner against this table (in microseconds), averaged
     * over all partitions
     */
    double closeScannerLatency();

    /**
     * @return the number of Scanner opens that were recorded
     */
    long numOpenEvents();

    /**
     * @return the number of Scanner closes that were recorded
     */
    long numCloseEvents();
}
