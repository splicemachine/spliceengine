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

package com.splicemachine.pipeline.callbuffer;

/**
 * @author Scott Fines
 * Created on: 8/28/13
 */
public interface BufferConfiguration {

    /**
     * @return the maximum heap space to allow the buffer to occupy before flushing
     */
    long getMaxHeapSize();

    /**
     * @return the maximum number of records to buffer before flushing
     */
    int getMaxEntries();

    /**
     * Note: Not all buffers are required to abide by this setting (if, for example, the buffer
     * does not do any per-region grouping, it will not obey this condition).
     *
     * @return the maximum number of concurrent flushes that are allowed for a given region.
     */
    int getMaxFlushesPerRegion();

    void writeRejected();
}
