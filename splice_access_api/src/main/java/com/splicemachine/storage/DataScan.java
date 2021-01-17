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

package com.splicemachine.storage;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.utils.Pair;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public interface DataScan extends Attributable{

    DataScan startKey(byte[] startKey);

    DataScan stopKey(byte[] stopKey);

    DataScan filter(DataFilter df);

    /**
     * Reverse the order in which this scan is operating.
     *
     * @return a scan which scans in reverse (i.e. descending order).
     */
    DataScan reverseOrder();

    boolean isDescendingScan();

    DataScan cacheRows(int rowsToCache);

    DataScan batchCells(int cellsToBatch);

    byte[] getStartKey();

    byte[] getStopKey();

    long highVersion();

    long lowVersion();

    DataFilter getFilter();

    void setTimeRange(long lowVersion,long highVersion);

    void returnAllVersions();

    void setSmall(boolean small);

    // Given a list of [startKey, endKey) pairs, add a filter to this DataScan
    // that that excludes rows not covered by any range.
    // Currently not supported on the mem platform, where attempting to add this
    // filter will throw an IOException.
    void addRowkeyRangesFilter(List<Pair<byte[],byte[]>> rowkeyPairs) throws IOException;

}
