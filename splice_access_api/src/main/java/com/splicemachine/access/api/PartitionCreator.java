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

    PartitionCreator withTransactionId(long txnId) throws IOException;

    PartitionCreator withSplitKeys(byte[][] splitKeys);

    PartitionCreator withCatalogVersion(String version);

    Partition create() throws IOException;

}
