package com.splicemachine.derby.stream.control;

import com.google.common.base.Charsets;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.IndexTableScannerBuilder;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.derby.stream.iterator.DirectScanner;
import com.splicemachine.derby.stream.iterator.DirectScannerIterator;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.Partition;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Local control side DataSetProcessor.
 *
 * @author jleach
 */
public class ControlDataSetProcessor implements DataSetProcessor{
    private int failBadRecordCount=-1;
    private boolean permissive;

    private static final Logger LOG=Logger.getLogger(ControlDataSetProcessor.class);

    private final TxnSupplier txnSupplier;
    private final IgnoreTxnCacheSupplier ignoreSupplier;
    private final Transactor transactory;
    private final TxnOperationFactory txnOperationFactory;

    public ControlDataSetProcessor(TxnSupplier txnSupplier,
                                   IgnoreTxnCacheSupplier ignoreSupplier,
                                   Transactor transactory,
                                   TxnOperationFactory txnOperationFactory){
        this.txnSupplier=txnSupplier;
        this.ignoreSupplier=ignoreSupplier;
        this.transactory=transactory;
        this.txnOperationFactory=txnOperationFactory;
    }

    @Override
    public <Op extends SpliceOperation,V> ScanSetBuilder<V> newScanSet(final Op spliceOperation,final String tableName) throws StandardException{
        return new TableScannerBuilder<V>(){
            @Override
            public DataSet<V> buildDataSet() throws StandardException{
                Partition p;
                try{
                    p =SIDriver.driver().getTableFactory().getTable(tableName);
                    TxnRegion localRegion=new TxnRegion(p,NoopRollForward.INSTANCE,NoOpReadResolver.INSTANCE,
                            txnSupplier,ignoreSupplier,transactory,txnOperationFactory);

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
    public <Op extends SpliceOperation,V> IndexScanSetBuilder<V> newIndexScanSet(final Op spliceOperation,final String tableName) throws StandardException{
       return new IndexTableScannerBuilder<V>(){
           @Override
           public DataSet<V> buildDataSet() throws StandardException{
               rowDecodingMap(indexColToMainColPosMap);
               Partition p;
               try{
                   p =SIDriver.driver().getTableFactory().getTable(tableName);
                   TxnRegion localRegion=new TxnRegion(p,NoopRollForward.INSTANCE,NoOpReadResolver.INSTANCE,
                           txnSupplier,ignoreSupplier,transactory,txnOperationFactory);

                   this.region(localRegion).scanner(p.openScanner(getScan())); //set the scanner
                   DirectScanner ds = new DirectScanner(scanner,region,txn,demarcationPoint,Metrics.noOpMetricFactory());
                   DirectScannerIterator iter = new DirectScannerIterator(ds);
                   if(spliceOperation!=null){
                       spliceOperation.registerCloseable(iter);
                       spliceOperation.registerCloseable(p);
                   }
                   return new ControlDataSet(iter);
               }catch(IOException e){
                   throw Exceptions.parseException(e);
               }
           }
       };
    }

    @Override
    public <V> DataSet<V> getEmpty(){
        return new ControlDataSet<>(Collections.<V>emptyList());
    }

    @Override
    public <V> DataSet<V> getEmpty(String name){
        return getEmpty();
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value){
        return new ControlDataSet<>(Collections.singletonList(value));
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value, Object caller) {
        return singleRowDataSet(value);
    }

    @Override
    public <K,V> PairDataSet<K, V> singleRowPairDataSet(K key,V value){
        return new ControlPairDataSet<>(Collections.singletonList(new Tuple2<>(key,value)));
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation){
        OperationContext<Op> operationContext=new ControlOperationContext<>(spliceOperation);
        spliceOperation.setOperationContext(operationContext);
        if(permissive){
            operationContext.setPermissive();
            operationContext.setFailBadRecordCount(failBadRecordCount);
        }
        return operationContext;
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation){
        throw new RuntimeException("not implemented");
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
        return new ControlDataSet<>(new Iterable<String>(){
            @Override
            public Iterator<String> iterator(){
                try{
                    InputStream is=getFileStream(s);
                    return new TextFileIterator(is);
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }
        });
    }



    @Override
    public DataSet<String> readTextFile(String s,SpliceOperation op){
        return readTextFile(s);
    }

    @Override
    public <K,V> PairDataSet<K, V> getEmptyPair(){
        Iterable<Tuple2<K, V>> ks=Collections.emptyList();
        return new ControlPairDataSet<>(ks);
    }

    @Override
    public <V> DataSet<V> createDataSet(Iterable<V> value){
        return new ControlDataSet<>(value);
    }

    @Override
    public <V> DataSet<V> createDataSet(Iterable<V> value, String name) {
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
    public void setPermissive(){
        permissive=true;
    }

    @Override
    public void setFailBadRecordCount(int failBadRecordCount){
        this.failBadRecordCount=failBadRecordCount;
    }

    @Override
    public void clearBroadcastedOperation() {
        // do nothing
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private InputStream newInputStream(DistributedFileSystem dfs,Path p,OpenOption... options) throws IOException{
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
}