/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
                return dsp.readTextFile(op,location,escaped,delimited,baseColumnMap,operationContext,qualifiers,null,execRow).flatMap(new TableScanQualifierFunction(operationContext,null));
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
