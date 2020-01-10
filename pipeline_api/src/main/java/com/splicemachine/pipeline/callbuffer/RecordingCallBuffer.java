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


import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.callbuffer.CallBuffer;

/**
 * Recording Call Buffer
 * 
 * @author Scott Fines
 *         Created on: 9/6/13
 */
public interface RecordingCallBuffer<E> extends CallBuffer<E>{
    long getTotalElementsAdded();

    long getTotalBytesAdded();

    long getTotalFlushes();

    double getAverageEntriesPerFlush();

    double getAverageSizePerFlush();

    CallBuffer<E> unwrap();

	WriteStats getWriteStats();
}
