/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.RemoteQueryClient;

import javax.annotation.Nullable;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public interface DataSetProcessorFactory{

    /**
     * Choose the best DataSet Processor for this operation, based on characteristics
     * of this operation itself.
     *
     * @param activation the activation for this query, or {@code null} if operated
     *                   outside of a direct query structure.
     * @param op the Operation for this query.
     * @return the most appropriate DataSetProcessor for this operation and architecture.
     */
    DataSetProcessor chooseProcessor(@Nullable Activation activation,@Nullable SpliceOperation op);

    /**
     * Get the Local DataSetProcessor for this architecture. If there are multiple
     * such processors, then choose the most effective local processor for this operation.
     *
     * @param activation the activation for this query, or {@code null} if operated
     *                   outside of a direct query structure.
     * @param op the Operation for this query.
     * @return the local DataSetProcessor for this operation and architecture.
     */
    DataSetProcessor localProcessor(@Nullable Activation activation,@Nullable SpliceOperation op);

    /**
     * Get the Bulk DataSetProcessor for this architecture. If there are multiple
     * such processors, then choose the most effective bulk processor for this operation.
     *
     * This processor is useful for bulk remote reads that don't need heavy processing on a distributed
     * engine, but we don't want to saturate the storage engine
     *
     * @param activation the activation for this query, or {@code null} if operated
     *                   outside of a direct query structure.
     * @param op the Operation for this query.
     * @return the local DataSetProcessor for this operation and architecture.
     */
    DataSetProcessor bulkProcessor(@Nullable Activation activation,@Nullable SpliceOperation op);

    /**
     * @return the distributed DataSetProcessor for this architecture.
     */
    DistributedDataSetProcessor distributedProcessor();

    RemoteQueryClient getRemoteQueryClient(SpliceBaseOperation operation);
}
