/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.storage.util;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;

import java.io.IOException;
import java.util.List;

public class NoPartitionInfoCache<T> implements PartitionInfoCache<T> {

    private static PartitionInfoCache INSTANCE = new NoPartitionInfoCache();

    public static <T> PartitionInfoCache<T> getInstance() {
        return INSTANCE;
    }
    
    @Override
    public void configure(SConfiguration configuration) {
    }

    @Override
    public void invalidate(T t) throws IOException {
    }

    @Override
    public void invalidate(byte[] tableName) throws IOException {
    }

    @Override
    public List<Partition> getIfPresent(T t) throws IOException {
        return null;
    }

    @Override
    public void put(T t, List<Partition> partitions) throws IOException {
    }
}
