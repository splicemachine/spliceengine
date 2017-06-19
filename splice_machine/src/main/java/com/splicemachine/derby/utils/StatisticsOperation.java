
/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.derby.utils;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DerbyOperationInformation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.function.MergeStatisticsFlatMapFunction;
import com.splicemachine.derby.stream.function.MergeStatisticsHolder;
import com.splicemachine.derby.stream.function.MergeStatisticsHolderFlatMapFunction;
import com.splicemachine.derby.stream.function.ReturnStatisticsFlatMapFunction;
import com.splicemachine.derby.stream.function.StatisticsFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by jleach on 2/27/17.
 */
public class StatisticsOperation extends SpliceBaseOperation {
    private static Logger LOG=Logger.getLogger(StatisticsOperation.class);

    protected ScanSetBuilder scanSetBuilder;
    protected String scope;
    protected boolean useSample;
    protected double sampleFraction;

    // serialization
    public StatisticsOperation(){}

    public StatisticsOperation(ScanSetBuilder scanSetBuilder, boolean useSample, double sampleFraction, String scope, Activation activation) throws StandardException {
        super(new DerbyOperationInformation(activation, 0, 0, 0));
        this.scanSetBuilder = scanSetBuilder;
        this.scope = scope;
        this.activation = activation;
        this.useSample = useSample;
        this.sampleFraction = sampleFraction;
        if (useSample) {
            scanSetBuilder.useSample(useSample).sampleFraction(sampleFraction);
        }
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        dsp.setSchedulerPool("admin");
        try {
            DataSet statsDataSet;
            OperationContext<StatisticsOperation> operationContext = dsp.createOperationContext(this);
            if (scanSetBuilder.getStoredAs() != null) {
                ScanSetBuilder builder = scanSetBuilder;
                String storedAs = scanSetBuilder.getStoredAs();
                if (storedAs.equals("T"))
                    statsDataSet = dsp.readTextFile(null, builder.getLocation(), builder.getDelimited(), null, builder.getColumnPositionMap(), null, null, null, builder.getTemplate(), useSample, sampleFraction);
                else if (storedAs.equals("P"))
                    statsDataSet = dsp.readParquetFile(builder.getColumnPositionMap(), builder.getLocation(), null, null, null, builder.getTemplate(), useSample, sampleFraction);
                else if (storedAs.equals("O"))
                    statsDataSet = dsp.readORCFile(builder.getColumnPositionMap(), builder.getLocation(), null, null, null, builder.getTemplate(), useSample, sampleFraction);
                else {
                    throw new UnsupportedOperationException("storedAs Type not supported -> " + storedAs);
                }
            } else {
                statsDataSet = scanSetBuilder.buildDataSet(scope);
            }
            DataSet stats = statsDataSet
                    .mapPartitions(
                            new StatisticsFlatMapFunction(operationContext, scanSetBuilder.getBaseTableConglomId(), scanSetBuilder.getColumnPositionMap(), scanSetBuilder.getTemplate()))
                    .mapPartitions(new MergeStatisticsFlatMapFunction());
            int partitions = stats.partitions() / 4;
            while (partitions > 1) {
                stats = stats.coalesce(partitions, true).mapPartitions(new MergeStatisticsHolderFlatMapFunction());
                partitions /= 4;
            }
            return stats.coalesce(1, true).mapPartitions(new MergeStatisticsHolderFlatMapFunction()).mapPartitions(new ReturnStatisticsFlatMapFunction());
        } catch (StandardException se) {
            throw se;
        }

    }

    @Override
    public String getName() {
        return "StatisticsCollection";
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return new int[0];
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return false;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "statistics " + scope;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.emptyList();
    }

    @Override
    public void openCore() throws StandardException{
        DataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        if (!isOlapServer()) {
            remoteQueryClient = EngineDriver.driver().processorFactory().getRemoteQueryClient(this);
            remoteQueryClient.submit();
            // Does Not Open Iterator by design, we want statistics
            // to batch up.
        } else {
            openCore(dsp);
        }
    }


    @Override
    public ExecRow getNextRowCore() throws StandardException{
        try{
            if (locatedRowIterator == null)
                locatedRowIterator = remoteQueryClient.getIterator(); // Blocking Implementation
            if(locatedRowIterator.hasNext()){
                locatedRow=locatedRowIterator.next();
                if(LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG,"getNextRowCore %s locatedRow=%s",this,locatedRow);
                return locatedRow.getRow();
            }
            locatedRow=null;
            if(LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"getNextRowCore %s locatedRow=%s",this,locatedRow);
            return null;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(scope);
        out.writeObject(scanSetBuilder);
        out.writeBoolean(useSample);
        out.writeDouble(sampleFraction);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        scope = (String) in.readObject();
        scanSetBuilder = (ScanSetBuilder) in.readObject();
        useSample = in.readBoolean();
        sampleFraction = in.readDouble();
    }

    public double getSampleFraction() {
        return sampleFraction;
    }

    public boolean getUseSample() {
        return useSample;
    }
}
