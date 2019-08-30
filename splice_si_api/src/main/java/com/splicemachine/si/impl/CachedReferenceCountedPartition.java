/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.si.impl;

import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.spark_project.guava.cache.Cache;

import java.io.IOException;

/**
 * Created by jyuan on 8/30/19.
 */
public class CachedReferenceCountedPartition {

    protected static final Logger LOG = Logger.getLogger(CachedReferenceCountedPartition.class);

    private Partition partition;
    private boolean evicted = false;

    public CachedReferenceCountedPartition(Partition partition) {
        this.partition = partition;
    }

    private long referenceCount = 1;

    /**
     * Increments the reference count by {@code 1}.
     *
     * @return this object
     */
    public synchronized boolean retain() {

        if (isValid()) {
            referenceCount++;
            return true;
        }
        else {
            return false;
        }
    }

    public synchronized void close() throws IOException {

        referenceCount--;

        if (referenceCount < 0) {
            SpliceLogUtils.warn(LOG, "refCount = %d. This is unexpected!", referenceCount);
        }
        assert referenceCount >= 0;

        if (referenceCount <= 0 && evicted) {
            partition.close();
        }
    }

    public synchronized void markAsEvicted() {
        evicted = true;
        try {
            close();
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized Partition unwrapDelegate() {
        return partition;
    }

    private boolean isValid() {
        return !partition.isClosed() && !partition.isClosing();
    }

}
