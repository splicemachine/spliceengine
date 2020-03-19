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

package com.splicemachine.derby.lifecycle;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.util.NetworkUtils;
import com.splicemachine.client.SpliceClient;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.ControlExecutionLimiter;
import com.splicemachine.db.iapi.sql.conn.ControlExecutionLimiterImpl;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.NoRowsOperation;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.control.ControlDataSetProcessor;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.RemoteQueryClient;
import com.splicemachine.derby.stream.spark.HregionDataSetProcessor;
import com.splicemachine.derby.stream.spark.SparkDataSetProcessor;
import com.splicemachine.hbase.RegionServerLifecycleObserver;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stream.RemoteQueryClientImpl;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class CostChoosingDataSetProcessorFactory implements DataSetProcessorFactory{
    private final SIDriver driver;

    private static final Logger LOG = Logger.getLogger(CostChoosingDataSetProcessorFactory.class);
    private final String hostname;

    public CostChoosingDataSetProcessorFactory(){
        driver = SIDriver.driver();
        hostname = NetworkUtils.getHostname(driver.getConfiguration());
    }

    @Override
    public DataSetProcessor chooseProcessor(@Nullable Activation activation,@Nullable SpliceOperation op){
        if(! allowsDistributedExecution()
                || op instanceof NoRowsOperation
                || op instanceof ConstantAction){
            /*
             * We can't run in distributed mode because of something that the engine decided that,
             * for whatever reason, it's not available at the moment, so we have to use
             * the local processor instead
             */
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "chooseProcessor(): localProcessor for op %s", op==null?"null":op.getName());
            return new ControlDataSetProcessor(driver.getTxnSupplier(), driver.getTransactor(), driver.getOperationFactory());
        }
        // If we've already committed to running on spark, due to running a substatement
        // of a statement chosen to run on spark, or for some other reason, stick with the decision.
        if (op.isOlapServer())
            return new SparkDataSetProcessor();

        if (((BaseActivation)activation).datasetProcessorType().isSpark()) {
            return new SparkDataSetProcessor();
        } else {
            return new ControlDataSetProcessor(driver.getTxnSupplier(), driver.getTransactor(), driver.getOperationFactory());
        }
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

    private boolean isHBase() {
        if(Thread.currentThread().getName().contains("DRDAConn")) return true; //we are on the derby execution thread
        else return RegionServerLifecycleObserver.isHbaseJVM;
    }

    private boolean allowsDistributedExecution(){ // corresponds to master_dataset isRunningOnSpark
        if (isHBase()) return true;
        if (SpliceClient.isClient()) return true;
        else if(Thread.currentThread().getName().startsWith("olap-worker")) return true; //we are on the OlapServer thread
        else if(Thread.currentThread().getName().contains("ScalaTest")) return true; //we are on the OlapServer thread
        else if(Thread.currentThread().getName().contains("Executor task launch worker")) return false; //we are definitely in spark
        else return SpliceClient.isClient(); //we can run in spark as long as are in the HBase JVM
    }

    @Override
    public RemoteQueryClient getRemoteQueryClient(SpliceBaseOperation operation) {
        return new RemoteQueryClientImpl(operation, hostname);
    }

    @Override
    public ControlExecutionLimiter getControlExecutionLimiter(Activation activation) throws StandardException {
        if (!isHBase())
            return ControlExecutionLimiter.NO_OP;
        DataSetProcessorType type = activation.getLanguageConnectionContext().getDataSetProcessorType();
        type = type.combine(((BaseActivation)activation).datasetProcessorType());
        if (type.isHinted() || type.isForced()) {
            // If we are forcing execution one way or the other, do nothing
            return ControlExecutionLimiter.NO_OP;
        }

        long rowsLimit = EngineDriver.driver().getConfiguration().getControlExecutionRowLimit();
        return new ControlExecutionLimiterImpl(rowsLimit);
    }
}
