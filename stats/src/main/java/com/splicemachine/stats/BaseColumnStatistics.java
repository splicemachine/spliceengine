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

package com.splicemachine.stats;

import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public abstract class BaseColumnStatistics<T extends Comparable<T>> implements ColumnStatistics<T> {
    protected int columnId;
    protected long totalBytes;
    protected long totalCount;
    protected long nullCount;
    protected long minCount;

    public BaseColumnStatistics(int columnId, long totalBytes, long totalCount, long nullCount,long minCount) {
        this.columnId = columnId;
        this.totalBytes = totalBytes;
        this.totalCount = totalCount;
        this.nullCount = nullCount;
        this.minCount = minCount;
    }

    public BaseColumnStatistics(int columnId, long totalBytes, long totalCount, long nullCount) {
        this(columnId,totalBytes,totalCount,nullCount,0l);
    }

    @Override public int columnId() { return columnId; }

    @Override
    public int avgColumnWidth() {
        if(totalCount<=0||totalCount==nullCount) return 0;
        return (int)(totalBytes/nonNullCount());
    }

    @Override public long nonNullCount() { return totalCount-nullCount; }
    @Override public long nullCount() { return nullCount; }
    @Override public long minCount() { return minCount; }

    @Override
    public float nullFraction() {
        if(totalCount<=0) return 0f;
        return ((float)nullCount)/totalCount;
    }

    @Override
    public long totalBytes() {
        return totalBytes;
    }

    protected static void write(ColumnStatistics<?> item,DataOutput output) throws IOException {
        assert item instanceof BaseColumnStatistics: "Cannot encode non-BaseColumnStatistics";
        BaseColumnStatistics bsc = (BaseColumnStatistics)item;
        output.writeInt(bsc.columnId);
        output.writeLong(bsc.totalBytes);
        output.writeLong(bsc.totalCount);
        output.writeLong(bsc.nullCount);
        output.writeLong(bsc.minCount);
    }

}
