package com.splicemachine.derby.stream.control;

import com.splicemachine.SQLConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

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

    private static final Logger LOG = Logger.getLogger(CostChoosingDataSetProcessorFactory.class);

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
        if(!distributedDataSetProcessor.allowsExecution()){
            /*
             * We can't run in distributed mode because of something that the engine decided that,
             * for whatever reason, it's not available at the moment, so we have to use
             * the local processor instead
             */
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "chooseProcessor(): localProcessor for op %s", op.getName());
            return localProcessor;
        }
        if(activation==null|| activation.getResultSet()==null){
           if(op==null){
               /*
                * We don't really know how expensive the operation is going to be, so we have
                * to play it safe and assume that it will be very expensive and require the
                * distributed engine.
                */
               if (LOG.isTraceEnabled())
                   SpliceLogUtils.trace(LOG, "chooseProcessor(): distributedDataSetProcessor for null op");
               return distributedDataSetProcessor;
           }else{
               estimatedCost = op.getEstimatedCost();
               estimatedRowCount = op.getEstimatedRowCount();
           }
        }else{
            estimatedCost = ((SpliceOperation)activation.getResultSet()).getEstimatedCost();
            estimatedRowCount= ((SpliceOperation)activation.getResultSet()).getEstimatedRowCount();
        }
        if(estimatedCost>localCostThreshold||estimatedRowCount>localRowCountThreshold){
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "chooseProcessor(): distributedDataSetProcessor based on cost for op %s", op);
            return distributedDataSetProcessor;
        }else{
            SpliceLogUtils.trace(LOG, "chooseProcessor(): localProcessor based on cost for op %s", op);
            return localProcessor;
        }
    }

    @Override
    public DataSetProcessor localProcessor(@Nullable Activation activation,@Nullable SpliceOperation op){
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "localProcessor(): localProcessor provided for op %s", op);
        return localProcessor;
    }

    @Override
    public DistributedDataSetProcessor distributedProcessor(){
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "distributedProcessor(): distributedDataSetProcessor provided");
        return distributedDataSetProcessor;
    }
}
