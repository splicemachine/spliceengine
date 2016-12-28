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

package com.splicemachine.derby.stream.control;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.stream.iapi.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Charsets;
import scala.Tuple2;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.storage.Partition;

/**
 * Local control side DataSetProcessor.
 *
 * @author jleach
 */
public class ControlDataSetProcessor implements DataSetProcessor{
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
        return Type.LOCAL;
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
                    TableScannerIterator tableScannerIterator=new TableScannerIterator(this,spliceOperation);
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
            operationContext.setPermissive(statusDirectory, importFileName, badRecordThreshold);
        }
        return operationContext;
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation){
        return new ControlOperationContext<>(null);
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
    public void clearBroadcastedOperation() {
        // do nothing
    }

    @Override
    public void stopJobGroup(String jobName) {
        // do nothing
    }

    @Override
    public Partitioner getPartitioner(DataSet<LocatedRow> dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder, int[] rightHashKeys) {
        return NOOP_PARTITIONER;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private InputStream newInputStream(DistributedFileSystem dfs,@Nonnull Path p,OpenOption... options) throws IOException{
        InputStream value = dfs.newInputStream(p,options);
        String s=p.getFileName().toString();
        assert s!=null;
        if(s.endsWith("gz")){
            //need to open up a decompressing inputStream
            value = new GZIPInputStream(value);
        }
        return value;
    }

    private InputStream getFileStream(String s) throws IOException{
        DistributedFileSystem dfs=SIDriver.driver().fileSystem();
        InputStream value;
        if(dfs.getInfo(s).isDirectory()){
            //we need to open a Stream against each file in the directory
            InputStream inputStream = null;
            boolean sequenced = false;
            try(DirectoryStream<Path> stream =Files.newDirectoryStream(dfs.getPath(s))){
                for(Path p:stream){
                    if(inputStream==null){
                        inputStream = newInputStream(dfs,p,StandardOpenOption.READ);
                    }else {
                        inputStream = new SequenceInputStream(inputStream,newInputStream(dfs,p,StandardOpenOption.READ));
                    }
                }
            }
            value = inputStream;
        }else{
            value = newInputStream(dfs,dfs.getPath(s),StandardOpenOption.READ);
        }
        return value;
    }

    @Override
    public <V> DataSet<V> readParquetFile(int[] baseColumnMap, String location, OperationContext context,Qualifier[][] qualifiers,DataValueDescriptor probeValue, ExecRow execRow) throws StandardException {
            DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
            return new ControlDataSet(proc.readParquetFile(baseColumnMap, location, context, qualifiers, probeValue,execRow).toLocalIterator());
   }

    @Override
    public <V> DataSet<V> readORCFile(int[] baseColumnMap, String location, OperationContext context,Qualifier[][] qualifiers,DataValueDescriptor probeValue, ExecRow execRow) throws StandardException {
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        return new ControlDataSet(proc.readORCFile(baseColumnMap,location,context,qualifiers,probeValue,execRow).toLocalIterator());
    }

    @Override
    public <V> DataSet<LocatedRow> readTextFile(SpliceOperation op, String location, String characterDelimiter, String columnDelimiter, int[] baseColumnMap, OperationContext context, ExecRow execRow) throws StandardException{
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        return new ControlDataSet(proc.readTextFile(op,location,characterDelimiter,columnDelimiter,baseColumnMap,context,execRow).toLocalIterator());
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
    public void createEmptyExternalFile(ExecRow execRow, int[] baseColumnMap, int[] partitionBy, String storageAs, String location) throws StandardException {
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        proc.createEmptyExternalFile(execRow,baseColumnMap,partitionBy,storageAs,location);
    }

    @Override
    public void refreshTable(String location) {
        DistributedDataSetProcessor proc = EngineDriver.driver().processorFactory().distributedProcessor();
        proc.refreshTable(location);
    }
}
