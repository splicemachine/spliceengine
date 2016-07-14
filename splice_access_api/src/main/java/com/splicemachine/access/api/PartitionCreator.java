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

package com.splicemachine.access.api;

import com.splicemachine.storage.Partition;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface PartitionCreator{

    PartitionCreator withName(String name);

    PartitionCreator withDisplayNames(String[] displayNames);

    /**
     * Set the maximum size of a given subpartition for this overall partition,
     * if the underlying architecture supports table-specific partition sizes.
     *
     * If the architecture does not support table-specific partition sizes, then
     * this is a no-op.
     *
     * @param partitionSize the size of a partition
     * @return a creator
     */
    PartitionCreator withPartitionSize(long partitionSize);

    PartitionCreator withCoprocessor(String coprocessor) throws IOException;

    Partition create() throws IOException;
}
