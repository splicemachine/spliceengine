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

package com.splicemachine.derby.stream.control;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.ExplainNode;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.storage.Partition;
import com.splicemachine.system.CsvOptions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import splice.com.google.common.base.Charsets;
import scala.Tuple2;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URISyntaxException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

import static com.splicemachine.db.impl.sql.compile.ExplainNode.SparkExplainKind.NONE;

/**
 * Local control side DataSetProcessor.
 *
 * @author jleach
 */
public class ControlDataSetProcessor implements DataSetProcessor{
    private static final String BAD_FILENAME = "unspecified_";
    private long badRecordThreshold=-1;
    private boolean permissive;
    private String statusDirectory;
    private String importFileName;

    private static final Logger LOG=Logger.getLogger(ControlDataSetProcessor.class);

    protected final TxnSupplier txnSupplier;
    protected final Transactor transactory;
    protected final TxnOperationFactory txnOperationFactory;

    public ControlDataSetProcessor(TxnSupplier txnSupplier,
                                   Transactor transactory,
                                   TxnOperationFactory txnOperationFactory){
        this.txnSupplier=txnSupplier;
        this.transactory=transactory;
        this.txnOperationFactory=txnOperationFactory;
    }

    @Override
    public Type getType() {
        return Type.CONTROL;
    }

    public static final Partitioner NOOP_PARTITIONER = new Partitioner() {
        @Override
        public void initialize() {
        }

        @Override
        public int numPartitions() {
            return 0;
        }

        @Override
        public int getPartition(Object o) {
            return 0;
        }
    };

    @Override
    @SuppressFBWarnings(value = "SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION",justification = "Serialization" +
            "of this is a mistake for control-side operations")
    public <Op extends SpliceOperation,V> ScanSetBuilder<V> newScanSet(final Op spliceOperation,final String tableName) throws StandardException{
        return new TableScannerBuilder<V>(){
            @Override
            public DataSet<V> buildDataSet() throws StandardException{
                Partition p;
                try{
                    p =SIDriver.driver().getTableFactory().getTable(tableName);
                    TxnRegion localRegion=new TxnRegion(p,NoopRollForward.INSTANCE,NoOpReadResolver.INSTANCE,
                            txnSupplier,transactory,txnOperationFactory);

                    this.region(localRegion).scanner(p.openScanner(getScan(),metricFactory)); //set the scanner
                    SpliceOperation scanOperation = (spliceOperation instanceof ScanOperation) ? spliceOperation : null;
                    TableScannerIterator tableScannerIterator=new TableScannerIterator(this, scanOperation);
                    if(spliceOperation!=null){
                        spliceOperation.registerCloseable(tableScannerIterator);
                        spliceOperation.registerCloseable(p);
                    }
                    return new ControlDataSet(tableScannerIterator);
                }catch(IOException e){
                    throw Exceptions.parseException(e);
                }
            }
        };
    }

    @Override
    public <V> DataSet<V> getEmpty(){
        return new ControlDataSet<>(Collections.<V>emptyList().iterator());
    }

    @Override
    public <V> DataSet<V> getEmpty(String name){
        return getEmpty();
    }

    @Override
    public <V> DataSet<V> getEmpty(String name, OperationContext context){
        return getEmpty();
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value){
        return new ControlDataSet<>(new SingletonIterator(value));
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value, Object caller) {
        return singleRowDataSet(value);
    }

    @Override
    public <K,V> PairDataSet<K, V> singleRowPairDataSet(K key,V value){
        return new ControlPairDataSet<>(new SingletonIterator(new Tuple2<>(key,value)));
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation){
        OperationContext<Op> operationContext=new ControlOperationContext<>(spliceOperation);
        spliceOperation.setOperationContext(operationContext);
        if(permissive){
            if(importFileName == null)importFileName=BAD_FILENAME + System.currentTimeMillis();
            operationContext.setPermissive(statusDirectory, importFileName, badRecordThreshold);
        }
        return operationContext;
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation){
        return new ControlOperationContext<>(activation);
    }

    @Override
    public void setJobGroup(String jobName,String jobDescription){
    }

    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String s,SpliceOperation op){
        try{
            InputStream is = getFileStream(s);
            return singleRowPairDataSet(s,is);
        }catch(IOException e){
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String s){
        return readWholeTextFile(s,null);
    }

    @Override
    public DataSet<String> readTextFile(final String s){
        try{
            InputStream is=getFileStream(s);
            return new ControlDataSet<>(new TextFileIterator(is));
        }catch(IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }



    @Override
    public DataSet<String> readTextFile(String s,SpliceOperation op){
        return readTextFile(s);
    }

    @Override
    public <K,V> PairDataSet<K, V> getEmptyPair(){
        return new ControlPairDataSet<>(Collections.<Tuple2<K, V>>emptyList().iterator());
    }

    @Override
    public <V> DataSet<V> createDataSet(Iterator<V> value){
        return new ControlDataSet<>(value);
    }

    @Override
    public <V> DataSet<V> createDataSet(Iterator<V> value, String name) {
        return new ControlDataSet<>(value);
    }

    @Override
    public void setSchedulerPool(String pool){
        // no op
    }

    private static class TextFileIterator implements Iterator<String>{

        Scanner scanner;

        public TextFileIterator(InputStream inputStream){
            //-sf- adding UTF-8 charset here to avoid findbugs warning. If we stop using UTF-8, we might be in trouble
            this.scanner=new Scanner(inputStream,Charsets.UTF_8.name());
        }

        @Override
        public void remove(){
        }

        @Override
        public String next(){
            return scanner.nextLine();
        }

        @Override
        public boolean hasNext(){
            return scanner.hasNextLine();
        }

    }

    @Override
    public void setPermissive(String statusDirectory, String importFileName, long badRecordThreshold){
        this.permissive = true;
        this.statusDirectory = statusDirectory;
        this.importFileName = importFileName;
        this.badRecordThreshold = badRecordThreshold;
    }

    @Override
    public void stopJobGroup(String jobName) {
        // do nothing
    }

    @Override
    public Partitioner getPartitioner(DataSet<ExecRow> dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder, int[] rightHashKeys) {
        return NOOP_PARTITIONER;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private InputStream newInputStream(DistributedFileSystem dfs,@Nonnull String p,OpenOption... options) throws IOException{
        assert p!=null;
        InputStream value = dfs.newInputStream(p,options);
        if(p.endsWith("gz")){
            //need to open up a decompressing inputStream
            value = new GZIPInputStream(value);
        }
        return value;
    }

    private InputStream getFileStream(String s) throws IOException, URISyntaxException {
        DistributedFileSystem dfs=SIDriver.driver().getSIEnvironment().fileSystem(s);
        InputStream value;
        if(dfs.getInfo(s).isDirectory()){
            //we need to open a Stream against each file in the directory
            InputStream inputStream = null;
            for (String file : dfs.getExistingFiles(s, "*")) {
                if(inputStream==null){
                    inputStream = newInputStream(dfs,file,StandardOpenOption.READ);
                }else {
                    inputStream = new SequenceInputStream(inputStream,newInputStream(dfs,file,StandardOpenOption.READ));
                }
            }
            value = inputStream;
        }else{
            value = newInputStream(dfs,s,StandardOpenOption.READ);
        }
        return value;
    }

    @Override
    public <V> DataSet<V> readParquetFile(StructType schema, int[] baseColumnMap, int[] partitionColumnMap,
                                          String location, OperationContext context,Qualifier[][] qualifiers,
                                          DataValueDescriptor probeValue, ExecRow execRow,
                                          boolean useSample, double sampleFraction) throws StandardException {
            DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
            return new ControlDataSet(proc.readParquetFile(schema, baseColumnMap,partitionColumnMap, location,
                    context, qualifiers, probeValue,execRow, useSample, sampleFraction).toLocalIterator());
   }

    @Override
    public <V> DataSet<V> readAvroFile(StructType schema, int[] baseColumnMap, int[] partitionColumnMap,
                                       String location, OperationContext context,Qualifier[][] qualifiers,
                                       DataValueDescriptor probeValue, ExecRow execRow, boolean useSample,
                                       double sampleFraction) throws StandardException {
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        return new ControlDataSet(proc.readAvroFile(schema, baseColumnMap,partitionColumnMap, location, context,
                qualifiers, probeValue,execRow, useSample, sampleFraction).toLocalIterator());
    }

    @Override
    public <V> DataSet<V> readORCFile(int[] baseColumnMap,int[] partitionColumnMap, String location, OperationContext context,Qualifier[][] qualifiers,DataValueDescriptor probeValue, ExecRow execRow,
                                      boolean useSample, double sampleFraction, boolean statsjob) throws StandardException {
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        return new ControlDataSet(proc.readORCFile(baseColumnMap,partitionColumnMap,location,context,qualifiers,probeValue,execRow, useSample, sampleFraction, statsjob).toLocalIterator());
    }

    @Override
    public <V> DataSet<ExecRow> readTextFile(SpliceOperation op, String location, CsvOptions csvOptions, int[] baseColumnMap,
                                                OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue, ExecRow execRow,
                                                boolean useSample, double sampleFraction) throws StandardException{
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        return new ControlDataSet(proc.readTextFile(op,location,csvOptions,baseColumnMap, context, qualifiers, probeValue, execRow, useSample, sampleFraction).toLocalIterator());
    }

    @Override
    public <V> DataSet<V> readPinnedTable(long conglomerateId,int[] baseColumnMap, String location, OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue, ExecRow execRow) throws StandardException {
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        return new ControlDataSet(proc.readPinnedTable(conglomerateId, baseColumnMap, location, context, qualifiers, probeValue,execRow).toLocalIterator());
    }

    @Override
    public void dropPinnedTable(long conglomerateId) throws StandardException {
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        proc.dropPinnedTable(conglomerateId);
    }

    @Override
    public void createEmptyExternalFile(StructField[] fields, int[] baseColumnMap, int[] partitionBy, String storageAs, String location, String compression) throws StandardException {
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        proc.createEmptyExternalFile(fields,baseColumnMap,partitionBy,storageAs,location, compression);
    }

    @Override
    public void refreshTable(String location) {
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        proc.refreshTable(location);
    }

    @Override
    public StructType getExternalFileSchema(String storedAs, String location, boolean mergeSchema, CsvOptions csvOptions) throws StandardException {
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        return proc.getExternalFileSchema(storedAs,location,mergeSchema, csvOptions);
    }

    @Override
    public Boolean isCached(long conglomerateId) throws StandardException {
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        return proc.isCached(conglomerateId);
    }
    
    @Override
    public TableChecker getTableChecker(String schemaName, String tableName, DataSet table,
                                        KeyHashDecoder tableKeyDecoder, ExecRow tableKey, TxnView txn, boolean fix,
                                        int[] baseColumnMap, boolean isSystemTable) {
        return new ControlTableChecker(schemaName, tableName, table, tableKeyDecoder, tableKey, txn, fix, baseColumnMap,
                isSystemTable);
    }

    // Operations specific to native spark explains
    // have no effect on control queries.
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
        throw new UnsupportedOperationException();
    }
}
