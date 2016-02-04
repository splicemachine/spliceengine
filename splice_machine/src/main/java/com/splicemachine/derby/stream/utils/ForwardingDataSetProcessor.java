package com.splicemachine.derby.stream.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.*;

import java.io.InputStream;

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
    public <Op extends SpliceOperation,V> IndexScanSetBuilder<V> newIndexScanSet(Op spliceOperation,String tableName) throws StandardException{
        return delegate.newIndexScanSet(spliceOperation,tableName);
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
    public <V> DataSet<V> singleRowDataSet(V value){
        return delegate.singleRowDataSet(value);
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value,SpliceOperation op,boolean isLast){
        return delegate.singleRowDataSet(value,op,isLast);
    }

    @Override
    public <V> DataSet<V> createDataSet(Iterable<V> value){
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
    public PairDataSet<String, InputStream> readWholeTextFile(String path){
        return delegate.readWholeTextFile(path);
    }

    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String path,SpliceOperation op){
        return delegate.readWholeTextFile(path,op);
    }

    @Override
    public DataSet<String> readTextFile(String path){
        return delegate.readTextFile(path);
    }

    @Override
    public DataSet<String> readTextFile(String path,SpliceOperation op){
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
    public void setPermissive(){
        delegate.setPermissive();
    }

    @Override
    public void setFailBadRecordCount(int failBadRecordCount){
        delegate.setFailBadRecordCount(failBadRecordCount);
    }

    @Override
    public void clearBroadcastedOperation(){
        //no-op
    }
}

