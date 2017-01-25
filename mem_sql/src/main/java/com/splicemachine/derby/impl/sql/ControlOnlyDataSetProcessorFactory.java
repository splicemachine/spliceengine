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

package com.splicemachine.derby.impl.sql;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.control.ControlDataSetProcessor;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.RemoteQueryClient;
import com.splicemachine.derby.stream.utils.ForwardingDataSetProcessor;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * A DataSetProcessor Factory which only generates Control-Side DataSet processors. This is because memory
 * cannot support spark.
 *
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class ControlOnlyDataSetProcessorFactory implements DataSetProcessorFactory{
    private final SIDriver driver;

    private static final Logger LOG = Logger.getLogger(ControlOnlyDataSetProcessorFactory.class);

    public ControlOnlyDataSetProcessorFactory(){
        driver=SIDriver.driver();
    }

    private ControlDataSetProcessor createControlDataSetProcessor() {
        return new ControlDataSetProcessor(driver.getTxnSupplier(),
                                           driver.getTransactor(),
                                           driver.getOperationFactory());
    }

    @Override
    public DataSetProcessor chooseProcessor(@Nullable Activation activation,@Nullable SpliceOperation op){
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "chooseProcessor(): ControlDataSetProcessor provided for op %s", op);
        return createControlDataSetProcessor();
    }

    @Override
    public DataSetProcessor localProcessor(@Nullable Activation activation,@Nullable SpliceOperation op){
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "localProcessor(): ControlDataSetProcessor provided for op %s", op);
        return createControlDataSetProcessor();
    }

    @Override
    public DataSetProcessor bulkProcessor(@Nullable Activation activation, @Nullable SpliceOperation op) {
        return createControlDataSetProcessor();
    }

    @Override
    public DistributedDataSetProcessor distributedProcessor(){
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "distributedProcessor(): DistributedWrapper provided");
        return new DistributedWrapper(createControlDataSetProcessor());
    }

    @Override
    public RemoteQueryClient getRemoteQueryClient(final SpliceBaseOperation operation) {
        return new RemoteQueryClient() {
            @Override
            public void submit() throws StandardException {
                operation.openCore(createControlDataSetProcessor());
            }

            @Override
            public Iterator<LocatedRow> getIterator() {
                return operation.getLocatedRowIterator();
            }

            @Override
            public void close() throws Exception {
                // no-op
            }
        };
    }

    private static class DistributedWrapper extends ForwardingDataSetProcessor implements DistributedDataSetProcessor{
        public DistributedWrapper(ControlDataSetProcessor cdsp){
            super(cdsp);
        }

        @Override
        public void setup(Activation activation,String description,String schedulerPool) throws StandardException{
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "DistributedWrapper#setup()");
            //no-op
        }

        @Override
        public void createEmptyExternalFile(ExecRow execRow, int[] baseColumnMap, int[] partitionBy, String storageAs, String location, String compression) throws StandardException {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "DistributedWrapper#createEmptyExternalFile()");
        }

        @Override
        public void refreshTable(String location) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "DistributedWrapper#refreshTable()");
        }

        @Override
        public StructType getExternalFileSchema(String storedAs, String location) {
            if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "DistributedWrapper#getExternalFileSchema()");
            //no-op
            return null;
        }
    }
}
