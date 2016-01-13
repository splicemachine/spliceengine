package com.splicemachine.derby.stream.control;

import com.splicemachine.access.api.DistributedFileOpenOption;
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
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.Partition;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.Scanner;

/**
 * Created by jleach on 4/13/15.
 */
public class ControlDataSetProcessor implements DataSetProcessor{
    private int failBadRecordCount=-1;
    private boolean permissive;

    private static final Logger LOG=Logger.getLogger(ControlDataSetProcessor.class);

    private final TxnSupplier txnSupplier;
    private final IgnoreTxnCacheSupplier ignoreSupplier;
    private final Transactor transactory;
    private final DataStore dataStore;
    private final TxnOperationFactory txnOperationFactory;

    public ControlDataSetProcessor(TxnSupplier txnSupplier,
                                   IgnoreTxnCacheSupplier ignoreSupplier,
                                   Transactor transactory,
                                   DataStore dataStore,
                                   TxnOperationFactory txnOperationFactory){
        this.txnSupplier=txnSupplier;
        this.ignoreSupplier=ignoreSupplier;
        this.transactory=transactory;
        this.dataStore=dataStore;
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
                            txnSupplier,ignoreSupplier,dataStore,transactory,txnOperationFactory);

                    this.region(localRegion).scanner(p.openScanner(getScan())); //set the scanner
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
                           txnSupplier,ignoreSupplier,dataStore,transactory,txnOperationFactory);

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

    //    @Override
//    public <V> DataSet<V> getHTableScanner(HTableScannerBuilder hTableBuilder, String tableName) throws StandardException {
//        TxnRegion localRegion = new TxnRegion(null, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
//                txnSupplier, ignoreSupplier,
//                dataStore, transactory);
//
//        hTableBuilder
//                .scanner(new ControlMeasuredRegionScanner(Bytes.toBytes(tableName),hTableBuilder.getScan()))
//                .region(localRegion)
//                .metricFactory(Metrics.noOpMetricFactory());
//        DirectScannerIterator tableScannerIterator = new DirectScannerIterator(hTableBuilder);
//        return new ControlDataSet<>(tableScannerIterator);
//    }

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
    public <V> DataSet<V> singleRowDataSet(V value,SpliceOperation op,boolean isLast){
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
        SIDriver driver = SIDriver.driver();
        DistributedFileSystem fssf = driver.fileSystem();
        try(InputStream is = fssf.newInputStream(fssf.getPath(s),StandardOpenOption.READ)){
            //TODO -sf- this almost certainly closes the InputStream too early...
            return singleRowPairDataSet(s,is);
        }catch(IOException e){
            throw new RuntimeException(e);
        }
//        Path path=new Path(s);
//        InputStream rawStream=null;
//        try{
//            CompressionCodecFactory factory=new CompressionCodecFactory(SpliceConstants.config);
//            CompressionCodec codec=factory.getCodec(path);
//            FileSystem fs=FileSystem.get(SpliceConstants.config);
//            FSDataInputStream fileIn=fs.open(path);
//            InputStream value;
//            if(codec!=null){
//                value=codec.createInputStream(fileIn);
//            }else{
//                value=fileIn;
//            }
//            return singleRowPairDataSet(s,value);
//        }catch(IOException e){
//            throw new RuntimeException(e);
//        }finally{
//            if(rawStream!=null){
//                try{
//                    rawStream.close();
//                }catch(IOException e){
//                    // ignore
//                }
//            }
//        }
    }

    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String s){
        return readWholeTextFile(s,null);
    }

    @Override
    public DataSet<String> readTextFile(String s){
        try{
            DistributedFileSystem dfs=SIDriver.driver().fileSystem();
            final InputStream value = dfs.newInputStream(dfs.getPath(s),StandardOpenOption.READ);
            return new ControlDataSet<>(new Iterable<String>(){
                @Override
                public Iterator<String> iterator(){
                    return new TextFileIterator(value);
                }
            });
        }catch(IOException e){
            throw new RuntimeException(e);
        }
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
    public void setSchedulerPool(String pool){
        // no op
    }

    private class TextFileIterator implements Iterator<String>{

        Scanner scanner;

        public TextFileIterator(InputStream inputStream){
            this.scanner=new Scanner(inputStream);
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

}