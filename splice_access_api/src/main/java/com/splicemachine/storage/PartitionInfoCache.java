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
}