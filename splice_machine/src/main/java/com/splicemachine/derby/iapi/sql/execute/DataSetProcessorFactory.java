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
