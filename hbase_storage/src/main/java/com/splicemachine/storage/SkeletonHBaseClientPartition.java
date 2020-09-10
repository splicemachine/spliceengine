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

package com.splicemachine.storage;

import com.google.protobuf.Service;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.util.MeasuredResultScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public abstract class SkeletonHBaseClientPartition implements Partition{

    @Override
    public String getName(){
        return getTableName();
    }


    @Override
    public String getEncodedName() {
        return getTableName();
    }

    /*Single row access methods*/
    @Override
    public DataResult get(DataGet get,DataResult previous) throws IOException{
        assert get instanceof HGet : "Programmer Error: incorrect type for performing a Get!";
        Result result=doGet(((HGet)get).unwrapDelegate());

        if(previous==null)
            previous = new HResult(result);
        else
            ((HResult)previous).set(result);
        return previous;
    }


    @Override
    public DataResult getFkCounter(byte[] key,DataResult previous) throws IOException{
        Get g = new Get(key);
        g.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.FK_COUNTER_COLUMN_BYTES);

        Result r = doGet(g);
        if(previous==null)
            previous = new HResult(r);
        else{
            ((HResult)previous).set(r);
        }
        return previous;
    }

    @Override
    public DataResult getLatest(byte[] key,DataResult previous) throws IOException{
        Get g = new Get(key);
        g.setMaxVersions(1);

        Result result=doGet(g);
        if(previous==null)
            previous = new HResult(result);
        else{
            ((HResult)previous).set(result);
        }
        return previous;
    }

    @Override
    public DataResult getLatest(byte[] rowKey,byte[] family,DataResult previous) throws IOException{
        Get g = new Get(rowKey);
        g.setMaxVersions(1);
        g.addFamily(family);

        Result result=doGet(g);
        if(previous==null)
            previous = new HResult(result);
        else{
            ((HResult)previous).set(result);
        }
        return previous;
    }

    /*Scan methods*/
    @Override
    public DataScanner openScanner(DataScan scan) throws IOException{
        return openScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan) throws IOException{
        return openResultScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataScanner openScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        MeasuredResultScanner scanner=new MeasuredResultScanner(getScanner(((HScan)scan).unwrapDelegate()),metricFactory);
        return new ListingResultScanner(this,scanner);
    }


    @Override
    public DataResultScanner openResultScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        MeasuredResultScanner scanner=new MeasuredResultScanner(getScanner(((HScan)scan).unwrapDelegate()),metricFactory);
        return new ResultDataScanner(scanner);
    }


    /*Data Mutation methods*/
    @Override
    public void put(DataPut put) throws IOException{
        doPut(((HPut)put).unwrapDelegate());
    }

    @Override
    public void delete(DataDelete delete) throws IOException{
        doDelete(((HDelete)delete).unwrapDelegate());
    }


    @Override
    public void delete(List<DataDelete> delete) throws IOException{
        List<Delete> deletes = new ArrayList<>();
        for (DataDelete dd : delete) {
            deletes.add(((HDelete)dd).unwrapDelegate());
        }
        doDelete(deletes);
    }

    @Override
    public void mutate(DataMutation put) throws IOException{
        if(put instanceof HPut)
            doPut(((HPut)put).unwrapDelegate());
        else doDelete(((HDelete)put).unwrapDelegate());
    }

    @Override
    public void batchMutate(List<DataMutation> mutations) {
        throw new UnsupportedOperationException("batchMutate not implemented");
    }

    @Override
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public Iterator<MutationStatus> writeBatch(DataPut[] toWrite) throws IOException{
        List<Put> puts = new ArrayList<>(toWrite.length);
        for(int i=0;i<toWrite.length;i++){
            puts.add(((HPut)toWrite[i]).unwrapDelegate());
        }
        doPut(puts);
        List<MutationStatus> results = new ArrayList<>(toWrite.length);
        for(int i=0;i<toWrite.length;i++){
            results.add(HMutationStatus.success());
        }

        return results.iterator();
    }

    //no-op for remote access
    @Override public void closeOperation() throws IOException{ }
    @Override public void startOperation() throws IOException{ }
    @Override public boolean isClosed(){ return false; }
    @Override public boolean isClosing(){ return false; }


    @Override
    public long increment(byte[] rowKey,byte[] family,byte[] qualifier,long amount) throws IOException{
        Increment incr = new Increment(rowKey);
        incr.addColumn(family,qualifier,amount);
        return doIncrement(incr);
    }

    @Override
    public Lock getRowLock(byte[] key,int keyOff,int keyLen) throws IOException{
        //we don't support distributed row locks--that would be hard
        throw new UnsupportedOperationException("Cannot support row locking with remote access");
    }
    /*
     * A Table represents all regions, so the range is (-Inf,Inf). Thus, it contains
     * everything.
     */
    @Override public byte[] getStartKey(){ return HConstants.EMPTY_START_ROW; }
    @Override public byte[] getEndKey(){ return HConstants.EMPTY_END_ROW; }
    @Override public boolean containsRow(byte[] row){ return true; }
    @Override public boolean containsRow(byte[] row,int offset,int length){ return true; }
    @Override public boolean overlapsRange(byte[] start,byte[] stop){ return true; }

    @Override
    public boolean overlapsRange(byte[] start,int startOff,int startLen,byte[] stop,int stopOff,int stopLen){
        return true;
    }

    @Override
    public void writesRequested(long writeRequests){
        //no-op
    }

    @Override
    public void readsRequested(long readRequests){
        //no-op
    }

    protected abstract Result doGet(Get get) throws IOException;

    protected abstract ResultScanner getScanner(Scan scan) throws IOException;

    protected abstract void doDelete(Delete delete) throws IOException;

    protected abstract void doDelete(List<Delete> delete) throws IOException;

    protected abstract void doPut(Put put) throws IOException;

    protected abstract void doPut(List<Put> puts) throws IOException;

    protected abstract long doIncrement(Increment incr) throws IOException;

    public abstract Table unwrapDelegate();

    public abstract  <T extends Service,V> Map<byte[],V> coprocessorExec(Class<T> serviceClass, Batch.Call<T,V> call) throws Throwable;

    public abstract <T extends Service,V> Map<byte[],V> coprocessorExec(
            Class<T> serviceClass,
            byte[] startKey,
            byte[] endKey,
            final Batch.Call<T,V> callable) throws Throwable;
}
