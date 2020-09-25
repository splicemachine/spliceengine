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

package com.splicemachine.derby.stream.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.ExplainNode;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.system.CsvOptions;
import org.apache.spark.sql.types.StructType;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import static com.splicemachine.db.impl.sql.compile.ExplainNode.SparkExplainKind.NONE;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public abstract class ForwardingDataSetProcessor implements DataSetProcessor{
    private final DataSetProcessor delegate;

    public ForwardingDataSetProcessor(DataSetProcessor delegate){
        this.delegate=delegate;
    }

    @Override
    public <Op extends SpliceOperation,V> ScanSetBuilder<V> newScanSet(Op spliceOperation,String tableName) throws StandardException{
        return delegate.newScanSet(spliceOperation,tableName);
    }

    @Override
    public Type getType() {
        return delegate.getType();
    }

    @Override
    public <V> DataSet<V> getEmpty(){
        return delegate.getEmpty();
    }

    @Override
    public <V> DataSet<V> getEmpty(String name){
        return delegate.getEmpty(name);
    }

    @Override
    public <V> DataSet<V> getEmpty(String name, OperationContext context){
        return delegate.getEmpty(name, context);
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value){
        return delegate.singleRowDataSet(value);
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value, Object caller) {
        return delegate.singleRowDataSet(value, caller);
    }

    @Override
    public <V> DataSet<V> createDataSet(Iterator<V> value){
        return delegate.createDataSet(value);
    }

    @Override
    public <V> DataSet<V> createDataSet(Iterator<V> value, String name) {
        return delegate.createDataSet(value);
    }

    @Override
    public <K,V> PairDataSet<K, V> singleRowPairDataSet(K key,V value){
        return delegate.singleRowPairDataSet(key,value);
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation){
        return delegate.createOperationContext(spliceOperation);
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation){
        return delegate.createOperationContext(activation);
    }

    @Override
    public void setJobGroup(String jobName,String jobDescription){
        delegate.setJobGroup(jobName,jobDescription);
    }

    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String path) throws StandardException{
        return delegate.readWholeTextFile(path);
    }

    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String path,SpliceOperation op) throws StandardException{
        return delegate.readWholeTextFile(path,op);
    }

    @Override
    public DataSet<String> readTextFile(String path) throws StandardException {
        return delegate.readTextFile(path);
    }

    @Override
    public DataSet<String> readTextFile(String path,SpliceOperation op) throws StandardException {
        return delegate.readTextFile(path,op);
    }

    @Override
    public <K,V> PairDataSet<K, V> getEmptyPair(){
        return delegate.getEmptyPair();
    }

    @Override
    public void setSchedulerPool(String pool){
        delegate.setSchedulerPool(pool);
    }

    @Override
    public void setPermissive(String statusDirectory, String importFileName, long badRecordThreshold){
        delegate.setPermissive(statusDirectory, importFileName, badRecordThreshold);
    }

    @Override
    public void stopJobGroup(String jobName) {
        delegate.stopJobGroup(jobName);
    }

    @Override
    public Partitioner getPartitioner(DataSet<ExecRow> dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder, int[] rightHashKeys) {
        return delegate.getPartitioner(dataSet, template, keyDecodingMap, keyOrder,rightHashKeys);
    }

    @Override
    public <V> DataSet<V> readParquetFile(StructType schema, int[] baseColumnMap, int[] partitionColumnMap,
                                          String location, OperationContext context, Qualifier[][] qualifiers,
                                          DataValueDescriptor probeValue, ExecRow execRow, boolean useSample,
                                          double sampleFraction) throws StandardException {
        return delegate.readParquetFile(schema, baseColumnMap, partitionColumnMap,location, context, qualifiers,
                probeValue,execRow, useSample, sampleFraction);
    }

    @Override
    public <V> DataSet<V> readAvroFile(StructType schema, int[] baseColumnMap,int[] partitionColumnMap,
                                       String location, OperationContext context, Qualifier[][] qualifiers,
                                       DataValueDescriptor probeValue,ExecRow execRow, boolean useSample,
                                       double sampleFraction) throws StandardException {
        return delegate.readAvroFile(schema, baseColumnMap, partitionColumnMap,location, context,qualifiers,probeValue,
                execRow, useSample, sampleFraction);
    }

    @Override
    public <V> DataSet<V> readORCFile(int[] baseColumnMap,int[] partitionColumnMap, String location, OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue,ExecRow execRow,
                                      boolean useSample, double sampleFraction, boolean statsjob) throws StandardException {
        return delegate.readORCFile(baseColumnMap, partitionColumnMap, location, context,qualifiers,probeValue,execRow, useSample, sampleFraction, statsjob);
    }

    @Override
    public <V> DataSet<ExecRow> readTextFile(SpliceOperation op, String location, CsvOptions csvOptions, int[] baseColumnMap, OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue,ExecRow execRow,
                                                boolean useSample, double sampleFraction) throws StandardException {
        return delegate.readTextFile(op, location, csvOptions, baseColumnMap, context,  qualifiers, probeValue, execRow, useSample, sampleFraction);
    }

    @Override
    public <V> DataSet<V> readPinnedTable(long conglomerateId, int[] baseColumnMap, String location, OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue, ExecRow execRow) throws StandardException {
        return delegate.readPinnedTable(conglomerateId, baseColumnMap, location, context, qualifiers, probeValue, execRow);
    }

    @Override
    public void dropPinnedTable(long conglomerateId) throws StandardException {
        delegate.dropPinnedTable(conglomerateId);
    }

    @Override
    public Boolean isCached(long conglomerateId) throws StandardException {
        return delegate.isCached(conglomerateId);
    }

    @Override
    public TableChecker getTableChecker(String schemaName, String tableName, DataSet tableDataSet,
                                        KeyHashDecoder decoder, ExecRow key, TxnView txn,  boolean fix,
                                        int[] baseColumnMap, boolean isSystemChecker) {
        return delegate.getTableChecker(schemaName, tableName, tableDataSet, decoder, key, txn, fix, baseColumnMap,
                isSystemChecker);
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

    @Override
    public <V> DataSet<ExecRow> readKafkaTopic(String topicName, OperationContext context) throws StandardException {
        return delegate.readKafkaTopic(topicName, context);
    }
}
