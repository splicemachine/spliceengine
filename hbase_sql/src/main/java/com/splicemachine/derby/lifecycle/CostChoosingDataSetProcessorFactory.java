package com.splicemachine.derby.lifecycle;

import javax.annotation.Nullable;

import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.stream.RemoteQueryClientImpl;
import org.apache.log4j.Logger;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.control.ControlDataSetProcessor;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.spark.HregionDataSetProcessor;
import com.splicemachine.derby.stream.spark.SparkDataSetProcessor;
import com.splicemachine.hbase.RegionServerLifecycleObserver;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.derby.stream.iapi.RemoteQueryClient;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class CostChoosingDataSetProcessorFactory implements DataSetProcessorFactory{
    private final SIDriver driver;

    private static final Logger LOG = Logger.getLogger(CostChoosingDataSetProcessorFactory.class);

    public CostChoosingDataSetProcessorFactory(){
        driver = SIDriver.driver();
    }

    @Override
    public DataSetProcessor chooseProcessor(@Nullable Activation activation,@Nullable SpliceOperation op){
        if(! allowsDistributedExecution()){
            /*
             * We can't run in distributed mode because of something that the engine decided that,
             * for whatever reason, it's not available at the moment, so we have to use
             * the local processor instead
             */
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "chooseProcessor(): localProcessor for op %s", op==null?"null":op.getName());
            return new ControlDataSetProcessor(driver.getTxnSupplier(), driver.getTransactor(), driver.getOperationFactory());
        }

        switch(activation.getLanguageConnectionContext().getDataSetProcessorType()){
            case FORCED_CONTROL:
                return new ControlDataSetProcessor(driver.getTxnSupplier(), driver.getTransactor(), driver.getOperationFactory());
            case FORCED_SPARK:
                return new SparkDataSetProcessor();
            default:
                break;
        }
        if (((BaseActivation)activation).useSpark())
            return new SparkDataSetProcessor();
        return new ControlDataSetProcessor(driver.getTxnSupplier(), driver.getTransactor(), driver.getOperationFactory());
    }

    @Override
    public DataSetProcessor localProcessor(@Nullable Activation activation,@Nullable SpliceOperation op){
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "localProcessor(): localProcessor provided for op %s", op==null?"null":op.getName());
        return new ControlDataSetProcessor(driver.getTxnSupplier(), driver.getTransactor(), driver.getOperationFactory());
    }

    @Override
    public DataSetProcessor bulkProcessor(@Nullable Activation activation, @Nullable SpliceOperation op) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "bulkProcessor(): bulkProcessor provided for op %s", op==null?"null":op.getName());
        if(! allowsDistributedExecution()){
            /*
             * We are running in a distributed node, use the bulk processor to avoid saturating HBase
             */
            return new HregionDataSetProcessor(driver.getTxnSupplier(), driver.getTransactor(), driver.getOperationFactory());
        } else {
            /*
             * We are running in control node, use a control side processor with less startup cost
             */
            return new ControlDataSetProcessor(driver.getTxnSupplier(), driver.getTransactor(), driver.getOperationFactory());

        }
    }

    @Override
    public DistributedDataSetProcessor distributedProcessor(){
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "distributedProcessor(): distributedDataSetProcessor provided");
        return new SparkDataSetProcessor();
    }

    private boolean allowsDistributedExecution(){ // corresponds to master_dataset isRunningOnSpark
        if(Thread.currentThread().getName().contains("DRDAConn")) return true; //we are on the derby execution thread
        else if(Thread.currentThread().getName().startsWith("olap-worker")) return true; //we are on the OlapServer thread
        else if(Thread.currentThread().getName().contains("Executor task launch worker")) return false; //we are definitely in spark
        else return RegionServerLifecycleObserver.isHbaseJVM; //we can run in spark as long as are in the HBase JVM
    }

    @Override
    public RemoteQueryClient getRemoteQueryClient(SpliceBaseOperation operation) {
        return new RemoteQueryClientImpl(operation);
    }
}
