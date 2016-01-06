package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;

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
     * @return the distributed DataSetProcessor for this architecture.
     */
    DistributedDataSetProcessor distributedProcessor();

}
