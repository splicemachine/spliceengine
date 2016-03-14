package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
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

    private static final Logger LOG = Logger.getLogger(CostChoosingDataSetProcessorFactory.class);

    public CostChoosingDataSetProcessorFactory(DistributedDataSetProcessor distributedDataSetProcessor,
                                               DataSetProcessor localProcessor){
        this.distributedDataSetProcessor=distributedDataSetProcessor;
        this.localProcessor=localProcessor;
    }

    @Override
    public DataSetProcessor chooseProcessor(@Nullable Activation activation,@Nullable SpliceOperation op){
        if(!distributedDataSetProcessor.allowsExecution()){
            /*
             * We can't run in distributed mode because of something that the engine decided that,
             * for whatever reason, it's not available at the moment, so we have to use
             * the local processor instead
             */
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "chooseProcessor(): localProcessor for op %s", op==null?"null":op.getName());
            return localProcessor;
        }

        switch(activation.getLanguageConnectionContext().getDataSetProcessorType()){
            case FORCED_CONTROL:
                return localProcessor;
            case FORCED_SPARK:
                return distributedDataSetProcessor;
            default:
                break;
        }
        if (((BaseActivation)activation).useSpark())
            return distributedDataSetProcessor;
        return localProcessor;

    }

    @Override
    public DataSetProcessor localProcessor(@Nullable Activation activation,@Nullable SpliceOperation op){
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "localProcessor(): localProcessor provided for op %s", op==null?"null":op.getName());
        return localProcessor;
    }

    @Override
    public DistributedDataSetProcessor distributedProcessor(){
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "distributedProcessor(): distributedDataSetProcessor provided");
        return distributedDataSetProcessor;
    }
}
