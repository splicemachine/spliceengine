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

package com.splicemachine.si.impl;

import splice.com.google.common.collect.Iterators;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.rollforward.RollForward;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import com.splicemachine.utils.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class TxnPartition implements Partition{
    private final Partition basePartition;
    private final Transactor transactor;
    private final RollForward rollForward;
    private final TxnOperationFactory txnOpFactory;
    private final TransactionReadController txnReadController;
    private final ReadResolver readResolver;

    public TxnPartition(Partition basePartition,
                        Transactor transactor,
                        RollForward rollForward,
                        TxnOperationFactory txnOpFactory,
                        TransactionReadController txnReadController,
                        ReadResolver readResolver){
        this.basePartition=basePartition;
        this.transactor=transactor;
        this.rollForward=rollForward;
        this.txnOpFactory=txnOpFactory;
        this.txnReadController=txnReadController;
        this.readResolver=readResolver;
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
    public String getEncodedName() {
        return basePartition.getEncodedName();
    }

    @Override
    public void close() throws IOException{
        basePartition.close();
    }

    @Override
    public DataResult get(DataGet get,DataResult previous) throws IOException{
        txnReadController.preProcessGet(get);
        TxnView txnView=txnOpFactory.fromReads(get);
        if(txnView!=null){
            get.setFilter(new MTxnFilterWrapper(getFilter(get,txnView)));
        }
        return basePartition.get(get,previous);
    }

    @Override
    public Iterator<DataResult> batchGet(Attributable attributes,List<byte[]> rowKeys) throws IOException{
        MGet get = new MGet();
        get.setAllAttributes(attributes.allAttributes());
        txnReadController.preProcessGet(get);
        attachFilterIfNeeded(get);

        List<DataResult> results = new ArrayList<>(rowKeys.size());
        for(byte[] key:rowKeys){
            get.setKey(key);
            results.add(basePartition.get(get,null));
        }
        return results.iterator();
    }


    @Override
    public DataScanner openScanner(DataScan scan) throws IOException{
        return openScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataScanner openScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        txnReadController.preProcessScan(scan);
        attachFilterIfNeeded(scan);
        return basePartition.openScanner(scan,metricFactory);
    }

    @Override
    public void put(DataPut put) throws IOException{
        transactor.processPut(basePartition,rollForward,put);
    }

    @Override
    public boolean checkAndPut(byte[] key,byte[] family,byte[] qualifier,byte[] expectedValue,DataPut put) throws IOException{
        return basePartition.checkAndPut(key,family,qualifier,expectedValue,put);
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
    public Iterator<MutationStatus> writeBatch(DataPut[] toWrite) throws IOException{
        return Iterators.forArray(transactor.processPutBatch(basePartition,rollForward,toWrite));
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
    public long increment(byte[] rowKey,byte[] family,byte[] qualifier,long amount) throws IOException{
        return basePartition.increment(rowKey,family,qualifier,amount);
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
    public DataResult getFkCounter(byte[] key,DataResult previous) throws IOException{
        return basePartition.getFkCounter(key,previous);
    }

    @Override
    public DataResult getLatest(byte[] key,DataResult previous) throws IOException{
        return basePartition.getLatest(key,previous);
    }

    @Override
    public Lock getRowLock(byte[] key,int keyOff,int keyLen) throws IOException{
        return basePartition.getRowLock(key,keyOff,keyLen);
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        attachFilterIfNeeded(scan);
        return basePartition.openResultScanner(scan,metricFactory);
    }


    @Override
    public DataResultScanner openResultScanner(DataScan scan) throws IOException{
        return openResultScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataResult getLatest(byte[] rowKey,byte[] family,DataResult previous) throws IOException{
        return basePartition.getLatest(rowKey,family,previous);
    }

    @Override
    public void delete(DataDelete delete) throws IOException{
        /*
         * In SI logic, we never physically delete things, except in the context of explicit physical management
         * (like Compaction in HBase, etc), so we trade this DataDelete in for a DataPut, and add a tombstone
         * instead.
         */
        TxnView txnView=txnOpFactory.fromWrites(delete);
        if(txnView==null)
            throw new IOException("Direct deletes are not supported under Snapshot Isolation");
        DataPut dp=txnOpFactory.newDataPut(txnView,delete.key());
        for(DataCell dc : delete.cells()){
            dp.tombstone(dc.version());
        }
        dp.setAllAttributes(delete.allAttributes());
        dp.addAttribute(SIConstants.SI_DELETE_PUT,SIConstants.TRUE_BYTES);

        put(dp);
    }

    @Override
    public void delete(List<DataDelete> delete) throws IOException {
        throw new UnsupportedOperationException("multi delete not supported");
    }

    @Override
    public void mutate(DataMutation put) throws IOException{
        if(put instanceof DataPut)
            put((DataPut)put);
        else delete((DataDelete)put);

    }

    @Override
    public void batchMutate(List<DataMutation> mutations) throws IOException {
        for (DataMutation dm : mutations) {
            mutate(dm);
        }
    }

    @Override
    public boolean containsRow(byte[] row){
        return this.basePartition.containsRow(row);
    }

    @Override
    public boolean containsRow(byte[] row,int offset,int length){
        return basePartition.containsRow(row,offset,length);
    }

    @Override
    public boolean overlapsRange(byte[] start,byte[] stop){
        return basePartition.overlapsRange(start,stop);
    }

    @Override
    public boolean overlapsRange(byte[] start,int startOff,int startLen,byte[] stop,int stopOff,int stopLen){
        return basePartition.overlapsRange(start,startOff,startLen,stop,stopOff,stopLen);
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
    public List<Partition> subPartitions(byte[] startRow,byte[] stopRow){
        return subPartitions(startRow,stopRow,false);
    }
    @Override
    public List<Partition> subPartitions(byte[] startRow, byte[] stopRow, boolean refresh) {
        if(!containsRow(startRow)||!containsRow(stopRow))
            throw new UnsupportedOperationException("Cannot get subpartitions of a range that it does not own!");
        return Collections.<Partition>singletonList(this);
    }


    @Override
    public PartitionLoad getLoad() throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void compact(boolean isMajor) throws IOException{
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

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private EntryPredicateFilter getEntryPredicateFilter(Attributable scan) throws IOException{
        byte[] epfBytes=scan.getAttribute(SIConstants.ENTRY_PREDICATE_LABEL);
        if(epfBytes==null) return null;

        return EntryPredicateFilter.fromBytes(epfBytes);
    }

    private TxnFilter getFilter(Attributable scan,TxnView txnView) throws IOException{
        EntryPredicateFilter epf=getEntryPredicateFilter(scan);
        TxnFilter txnFilter;
        if(epf!=null){
            txnFilter=txnReadController.newFilterStatePacked(readResolver,epf,txnView,false);
        }else{
            txnFilter=txnReadController.newFilterState(readResolver,txnView);
        }
        return txnFilter;
    }

    private void attachFilterIfNeeded(DataGet get) throws IOException{
        if(get.getAttribute(SIConstants.SI_NEEDED)!=null){
            TxnView txnView=txnOpFactory.fromReads(get);
            if(txnView!=null){
                TxnFilter txnFilter=getFilter(get,txnView);
                get.setFilter(new MTxnFilterWrapper(txnFilter));
            }
        }
    }

    private void attachFilterIfNeeded(DataScan scan) throws IOException{
        if(scan.getAttribute(SIConstants.SI_NEEDED)!=null){
            TxnView txnView=txnOpFactory.fromReads(scan);
            if(txnView!=null){
                TxnFilter txnFilter=getFilter(scan,txnView);
                scan.filter(new MTxnFilterWrapper(txnFilter));
            }
        }
    }

    @Override
    public BitSet getBloomInMemoryCheck(boolean hasConstraintChecker,Pair<KVPair, Lock>[] dataAndLocks) throws IOException {
        return null;
    }

    @Override
    public PartitionDescriptor getDescriptor() throws IOException {
        throw new UnsupportedOperationException("Operation not supported in mem storage engine");
    }
}
