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

import com.splicemachine.access.HConfiguration;
import com.splicemachine.client.SpliceClient;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.function.TableScanPredicateFunction;
import com.splicemachine.derby.stream.function.TableScanTupleMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.derby.stream.utils.AvroUtils;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMInputFormat;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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
            return dsp.readPinnedTable(Long.parseLong(tableName),baseColumnMap,location,operationContext,operation.getScanInformation().getScanQualifiers(),null,operation.getExecRowDefinition()).filter(new TableScanPredicateFunction(operationContext));
        }
        if (storedAs!= null) {
            ScanOperation operation = op==null?null:(ScanOperation) op;
            ExecRow execRow = operation==null?template:op.getExecRowDefinition();
            Qualifier[][] qualifiers = operation == null?null:operation.getScanInformation().getScanQualifiers();
            DataSet locatedRows;
            if (storedAs.equals("T"))
                locatedRows = dsp.readTextFile(op,location,escaped,delimited,baseColumnMap,operationContext,qualifiers,null,execRow, useSample, sampleFraction);
            else if (storedAs.equals("P"))
                locatedRows = dsp.readParquetFile(baseColumnMap,partitionByColumns,location,operationContext,qualifiers,null,execRow, useSample, sampleFraction);
            else if (storedAs.equals("A")) {
                AvroUtils.supportAvroDateTypeColumns(execRow);
                locatedRows = dsp.readAvroFile(baseColumnMap, partitionByColumns, location, operationContext, qualifiers, null, execRow, useSample, sampleFraction);
            }
            else if (storedAs.equals("O"))
                locatedRows = dsp.readORCFile(baseColumnMap,partitionByColumns,location,operationContext,qualifiers,null,execRow, useSample, sampleFraction, false);
            else {
                throw new UnsupportedOperationException("storedAs Type not supported -> " + storedAs);
            }
            return qualifiers == null?locatedRows:locatedRows.filter(new TableScanPredicateFunction<>(operationContext));
        }

        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(HConfiguration.unwrapDelegate());
        conf.set(MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName);
        if (SpliceClient.isClient())
            conf.set(MRConstants.SPLICE_CONNECTION_STRING, SpliceClient.connectionString);
        if (oneSplitPerRegion) {
            conf.set(MRConstants.ONE_SPLIT_PER_REGION, "true");
        }
        if (op != null) {
            ScanOperation sop = (ScanOperation) op;
            conf.setInt(MRConstants.SPLICE_SPLITS_PER_TABLE, sop.getSplits());
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
        SparkSpliceFunctionWrapper f = new SparkSpliceFunctionWrapper(new TableScanTupleMapFunction<SpliceOperation>(operationContext));
        SparkSpliceFunctionWrapper pred = new SparkSpliceFunctionWrapper(new TableScanPredicateFunction<>(operationContext,this.optionalProbeValue));
        SpliceSpark.pushScope(String.format("%s: Deserialize", scopePrefix));
        try {
            return new SparkDataSet<>(useSample?rawRDD.map(f).filter(pred).sample(false, sampleFraction):rawRDD.map(f).filter(pred),
                                      op != null ? op.getPrettyExplainPlan() : f.getPrettyFunctionName());
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeUTF(tableName);
        out.writeObject(dsp);
        out.writeObject(op);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        this.tableName = in.readUTF();
        this.dsp = (SparkDataSetProcessor)in.readObject();
        this.op = (SpliceOperation)in.readObject();
    }

}
