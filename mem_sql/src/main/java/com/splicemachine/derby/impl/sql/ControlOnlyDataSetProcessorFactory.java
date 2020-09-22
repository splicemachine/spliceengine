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

package com.splicemachine.derby.impl.sql;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.ControlExecutionLimiter;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.compile.ExplainNode;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.control.ControlDataSetProcessor;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.RemoteQueryClient;
import com.splicemachine.derby.stream.utils.ForwardingDataSetProcessor;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.system.CsvOptions;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

import static com.splicemachine.db.impl.sql.compile.ExplainNode.SparkExplainKind.NONE;

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
            public Iterator<ExecRow> getIterator() {
                return operation.getExecRowIterator();
            }

            @Override
            public void interrupt() {
                // no-op
            }

            @Override
            public void close() throws Exception {
                // no-op
            }
        };
    }

    @Override
    public ControlExecutionLimiter getControlExecutionLimiter(Activation activation) {
        return ControlExecutionLimiter.NO_OP;
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
        public void createEmptyExternalFile(StructField[] fields, int[] baseColumnMap, int[] partitionBy, String storageAs, String location, String compression) throws StandardException {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "DistributedWrapper#createEmptyExternalFile()");
        }

        @Override
        public void refreshTable(String location) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "DistributedWrapper#refreshTable()");
        }

        @Override
        public StructType getExternalFileSchema(String storedAs, String location, boolean mergeSchema, CsvOptions csvOptions) {
            if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "DistributedWrapper#getExternalFileSchema()");
            //no-op
            return null;
        }

        @Override
        public Boolean isCached(long conglomerateId) throws StandardException {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "DistributedWrapper#isCached()");
            //no-op
            return false;
        }

        // Operations specific to native spark explains
        // have no effect on non-spark queries.
        @Override public boolean isSparkExplain() { return false; }
        @Override public ExplainNode.SparkExplainKind getSparkExplainKind() { return NONE; }
        @Override public void setSparkExplain(ExplainNode.SparkExplainKind newValue) {  }

        @Override public void prependSpliceExplainString(String explainString) { }
        @Override public void appendSpliceExplainString(String explainString) { }
        @Override public void prependSparkExplainStrings(List<String> stringsToAdd, boolean firstOperationSource, boolean lastOperationSource) { }
        @Override public void popSpliceOperation() { }
        @Override public void finalizeTempOperationStrings() { }

        @Override public List<String> getNativeSparkExplain() { return null; }
        @Override public int getOpDepth() { return 0; }
        @Override public void incrementOpDepth() { }
        @Override public void decrementOpDepth() { }
        @Override public void resetOpDepth() { }
    }
}
