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

package com.splicemachine.derby.stream.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.stream.iapi.*;
import java.io.InputStream;
import java.util.Iterator;

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
    public void clearBroadcastedOperation(){
        //no-op
    }

    @Override
    public void stopJobGroup(String jobName) {
        delegate.stopJobGroup(jobName);
    }

    @Override
    public Partitioner getPartitioner(DataSet<LocatedRow> dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder, int[] rightHashKeys) {
        return delegate.getPartitioner(dataSet, template, keyDecodingMap, keyOrder,rightHashKeys);
    }
}

