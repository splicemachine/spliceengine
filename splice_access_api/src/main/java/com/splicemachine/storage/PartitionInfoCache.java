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

import com.splicemachine.access.api.SConfiguration;
import java.io.IOException;
import java.util.List;

/**
 * API reference for caching partition information.
 *
 * @author Scott Fines
 *         Date: 12/29/15
 */
public interface PartitionInfoCache<TableInfo>{
    void configure(SConfiguration configuration);
    void invalidate(TableInfo tableInfo) throws IOException;
    void invalidate(byte[] tableName) throws IOException;
    List<Partition> getIfPresent(TableInfo tableInfo) throws IOException;
    void put(TableInfo tableInfo, List<Partition> partitions) throws IOException;

    void invalidateAdapter(TableInfo tableInfo) throws IOException;
    void invalidateAdapter(byte[] tableName) throws IOException;
    List<Partition> getAdapterIfPresent(TableInfo tableInfo) throws IOException;
    void putAdapter(TableInfo tableInfo, List<Partition> partitions) throws IOException;
}
