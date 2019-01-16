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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * In the OrcFileWriter we see a lot of contention on the MemoryManager.
 * Use this MemoryManager to avoid the contention if you won't need the MemoryManager.
 */
public class NullMemoryManager
        extends MemoryManager
{
    public NullMemoryManager(Configuration conf)
    {
        super(conf);
    }

    @Override
    void addWriter(Path path, long requestedAllocation, Callback callback) {}

    @Override
    void removeWriter(Path path) {}

    @Override
    long getTotalMemoryPool()
    {
        return 0;
    }

    @Override
    double getAllocationScale()
    {
        return 0;
    }

    @Override
    void addedRow() {}
}