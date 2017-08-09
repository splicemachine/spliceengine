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

package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.IsolationLevel;
import com.splicemachine.si.api.txn.Txn;
import org.spark_project.guava.collect.Iterators;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.storage.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class TxnPartition implements Partition<byte[],Txn,IsolationLevel>{
    private final Partition basePartition;
    private final Transactor transactor;
    private final TxnOperationFactory txnOpFactory;

    public TxnPartition(Partition basePartition,
                        Transactor transactor,
                        TxnOperationFactory txnOpFactory){
        this.basePartition=basePartition;
        this.transactor=transactor;
        this.txnOpFactory=txnOpFactory;
    }

    @Override
    public String getTableName(){
        return basePartition.getTableName();
    }

    @Override
    public String getName(){
        return basePartition.getName();
    }

    @Override
    public void close() throws IOException{
        basePartition.close();
    }

    @Override
    public Record get(byte[] key, Txn txn, IsolationLevel isolationLevel) throws IOException{
        return basePartition.get(key,txn,isolationLevel);
    }

    @Override
    public Iterator<Record> batchGet(List<byte[]> rowKeys, Txn txn, IsolationLevel isolationLevel) throws IOException{
        List<Record> records = new ArrayList<>(rowKeys.size());
        for (byte[] rowKey: rowKeys) {
            records.add(get(rowKey,txn,isolationLevel));
        }
        return records.iterator();
    }


    @Override
    public RecordScanner openScanner(RecordScan scan, Txn txn, IsolationLevel isolationLevel) throws IOException{
        return basePartition.openScanner(scan,txn,isolationLevel);
    }

    @Override
    public void insert(Record record, Txn txn) throws IOException{
        transactor.processRecord(basePartition,record);
    }

    @Override
    public boolean checkAndPut(byte[] key,Record expectedValue, Record record) throws IOException{
        return basePartition.checkAndPut(key,expectedValue,record);
    }

    @Override
    public void startOperation() throws IOException{
        basePartition.startOperation();
    }

    @Override
    public void closeOperation() throws IOException{
        basePartition.closeOperation();
    }

    @Override
    public Iterator<MutationStatus> writeBatch(List<Record> toWrite) throws IOException{
        return Iterators.forArray(transactor.processRecordBatch(basePartition,toWrite.toArray(new Record[toWrite.size()])));
    }

    @Override
    public byte[] getStartKey(){
        return basePartition.getStartKey();
    }

    @Override
    public byte[] getEndKey(){
        return basePartition.getEndKey();
    }

    @Override
    public long increment(byte[] rowKey,long amount) throws IOException{
        return basePartition.increment(rowKey,amount);
    }

    @Override
    public long getCurrentIncrement(byte[] rowKey) throws IOException{
        return basePartition.getCurrentIncrement(rowKey);
    }

    @Override
    public boolean isClosed(){
        return basePartition.isClosed();
    }

    @Override
    public boolean isClosing(){
        return basePartition.isClosing();
    }

    @Override
    public Lock getRowLock(byte[] key) throws IOException{
        return basePartition.getRowLock(key);
    }

    @Override
    public Lock getRowLock(byte[] key,int keyOffset,int keyLength) throws IOException{
        return basePartition.getRowLock(key,keyOffset,keyLength);
    }

    @Override
    public void delete(byte[] key, Txn txn) throws IOException{
        basePartition.delete(key,txn);
    }

    @Override
    public void mutate(Record record, Txn txn) throws IOException{
        basePartition.mutate(record,txn);
    }

    @Override
    public boolean containsKey(byte[] row){
        return this.basePartition.containsKey(row);
    }

    @Override
    public boolean containsKey(byte[] row,long offset,int length){
        return basePartition.containsKey(row,offset,length);
    }

    @Override
    public boolean overlapsKeyRange(byte[] start,byte[] stop){
        return basePartition.overlapsKeyRange(start,stop);
    }

    @Override
    public boolean overlapsKeyRange(byte[] start,long startOff,int startLen,byte[] stop,long stopOff,int stopLen){
        return basePartition.overlapsKeyRange(start,startOff,startLen,stop,stopOff,stopLen);
    }

    @Override
    public void writesRequested(long writeRequests){
        basePartition.writesRequested(writeRequests);
    }

    @Override
    public void readsRequested(long readRequests){
        basePartition.readsRequested(readRequests);
    }

    @Override
    public List<Partition> subPartitions(){
        return subPartitions(false);
    }

    @Override
    public List<Partition> subPartitions(boolean refresh){
        return Collections.<Partition>singletonList(this);
    }

    @Override
    public PartitionServer owningServer(){
        return basePartition.owningServer();
    }

    @Override
    public List<Partition> subPartitions(byte[] startRow, byte[] stopRow, boolean refresh) {
        if(!containsKey(startRow)||!containsKey(stopRow))
            throw new UnsupportedOperationException("Cannot get subpartitions of a range that it does not own!");
        return Collections.<Partition>singletonList(this);
    }


    @Override
    public PartitionLoad getLoad() throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void compact() throws IOException{
        //no-op--memory storage does not perform compactions yet
    }

    @Override
    public void flush() throws IOException{
        //no-op--memory storage does not perform flush yet
    }

    @Override
    public String toString(){
        return getName()+"["+getTableName()+"]";
    }

    @Override
    public BitSet getBloomInMemoryCheck(Record[] records, Lock[] locks) throws IOException {
        return null;
    }
}
