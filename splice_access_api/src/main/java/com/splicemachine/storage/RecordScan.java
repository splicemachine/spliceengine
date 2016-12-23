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

package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public interface RecordScan<K> {

    RecordScan startKey(K startKey);

    RecordScan stopKey(K stopKey);

    /**
     * Reverse the order in which this scan is operating.
     *
     * @return a scan which scans in reverse (i.e. descending order).
     */
    RecordScan reverseOrder();

    boolean isDescendingScan();

    RecordScan cacheRows(int rowsToCache);

    RecordScan batchCells(int cellsToBatch);

    K getStartKey();

    K getStopKey();

}
