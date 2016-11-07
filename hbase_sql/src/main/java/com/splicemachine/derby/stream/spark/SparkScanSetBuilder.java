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

package com.splicemachine.derby.stream.spark;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.stream.function.TableScanQualifierFunction;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.function.TableScanTupleFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMInputFormat;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkScanSetBuilder<V> extends TableScannerBuilder<V> {
    private String tableName;
    private SparkDataSetProcessor dsp;
    private SpliceOperation op;

    public SparkScanSetBuilder(){
    }

    public SparkScanSetBuilder(SparkDataSetProcessor dsp,String tableName,SpliceOperation op){
        this.tableName=tableName;
        this.dsp = dsp;
        this.op = op;
    }

    @Override
    public DataSet<V> buildDataSet() throws StandardException {
        return buildDataSet("Scan Table");
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSet<V> buildDataSet(Object caller) throws StandardException {
        if (op != null)
            operationContext = dsp.createOperationContext(op);
        else // this call works even if activation is null
            operationContext = dsp.createOperationContext(activation);

        if (pin) {
            ScanOperation operation = (ScanOperation) op;
            return dsp.readPinnedTable(Long.parseLong(tableName),baseColumnMap,location,operationContext,operation.getScanInformation().getScanQualifiers(),null,operation.getExecRowDefinition()).flatMap(new TableScanQualifierFunction(operationContext,null));
        }
        if (storedAs!= null) {
            ScanOperation operation = op==null?null:(ScanOperation) op;
            ExecRow execRow = operation==null?template:op.getExecRowDefinition();
            Qualifier[][] qualifiers = operation == null?null:operation.getScanInformation().getScanQualifiers();
            if (storedAs.equals("T"))
                return dsp.readTextFile(op,location,escaped,delimited,baseColumnMap,operationContext,execRow).flatMap(new TableScanQualifierFunction(operationContext,null));
            if (storedAs.equals("P"))
                return dsp.readParquetFile(baseColumnMap,location,operationContext,qualifiers,null,operation.getExecRowDefinition()).flatMap(new TableScanQualifierFunction(operationContext,null));
            if (storedAs.equals("O"))
                return dsp.readORCFile(baseColumnMap,location,operationContext,qualifiers,null,operation.getExecRowDefinition()).flatMap(new TableScanQualifierFunction(operationContext,null));
            else {
                throw new UnsupportedOperationException("storedAs Type not supported -> " + storedAs);
            }
        }

        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(HConfiguration.unwrapDelegate());
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName);
        if (oneSplitPerRegion) {
            conf.set(MRConstants.ONE_SPLIT_PER_REGION, "true");
        }
        try {
             conf.set(MRConstants.SPLICE_SCAN_INFO,getTableScannerBuilderBase64String());
             conf.set(MRConstants.SPLICE_OPERATION_CONTEXT,  Base64.encodeBase64String(org.apache.commons.lang3.SerializationUtils.serialize(operationContext)));
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }

        String scopePrefix = StreamUtils.getScopeString(caller);
        SpliceSpark.pushScope(String.format("%s: Scan", scopePrefix));
        JavaPairRDD<RowLocation, ExecRow> rawRDD = ctx.newAPIHadoopRDD(
            conf, SMInputFormat.class, RowLocation.class, ExecRow.class);
        // rawRDD.setName(String.format(SparkConstants.RDD_NAME_SCAN_TABLE, tableDisplayName));
        rawRDD.setName("Perform Scan");
        SpliceSpark.popScope();
        SparkFlatMapFunction f = new SparkFlatMapFunction(new TableScanTupleFunction<SpliceOperation>(operationContext,this.optionalProbeValue));
        SpliceSpark.pushScope(String.format("%s: Deserialize", scopePrefix));
        try {
            return new SparkDataSet<>(rawRDD.flatMap(f), op != null ? op.getPrettyExplainPlan() : f.getPrettyFunctionName());
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeUTF(tableName);
        out.writeObject(dsp);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        this.tableName = in.readUTF();
        this.dsp = (SparkDataSetProcessor)in.readObject();
    }
}
