package com.splicemachine.derby.stream.control;

import com.splicemachine.SQLConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;

import javax.annotation.Nullable;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class CostChoosingDataSetProcessorFactory implements DataSetProcessorFactory{
    private final DistributedDataSetProcessor distributedDataSetProcessor;
    private final DataSetProcessor localProcessor;

    private final double localCostThreshold;
    private final double localRowCountThreshold;

    public CostChoosingDataSetProcessorFactory(DistributedDataSetProcessor distributedDataSetProcessor,
                                               DataSetProcessor localProcessor,
                                               SConfiguration configuration){
        this.distributedDataSetProcessor=distributedDataSetProcessor;
        this.localProcessor=localProcessor;

        this.localCostThreshold = configuration.getDouble(SQLConfiguration.CONTROL_SIDE_COST_THRESHOLD);
        this.localRowCountThreshold = configuration.getDouble(SQLConfiguration.CONTROL_SIDE_ROWCOUNT_THRESHOLD);
    }

    @Override
    public DataSetProcessor chooseProcessor(@Nullable Activation activation,@Nullable SpliceOperation op){
        double estimatedCost;
        double estimatedRowCount;
        if(activation==null|| activation.getResultSet()==null){
           if(op==null){
               /*
                * We don't really know how expensive the operation is going to be, so we have
                * to play it safe and assume that it will be very expensive and require the
                * distributed engine.
                */
               return distributedDataSetProcessor;
           }else{
               estimatedCost = op.getEstimatedCost();
               estimatedRowCount = op.getEstimatedRowCount();
           }
        }else{
            estimatedCost = ((SpliceOperation)activation.getResultSet()).getEstimatedCost();
            estimatedRowCount= ((SpliceOperation)activation.getResultSet()).getEstimatedRowCount();
        }
        return (estimatedCost > localCostThreshold || estimatedRowCount > localRowCountThreshold) ?distributedDataSetProcessor:localProcessor;
    }

    @Override
    public DataSetProcessor localProcessor(@Nullable Activation activation,@Nullable SpliceOperation op){
        return localProcessor;
    }

    @Override
    public DistributedDataSetProcessor distributedProcessor(){
        return distributedDataSetProcessor;
    }
}
