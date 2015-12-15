package com.splicemachine.si.impl;

import com.google.common.collect.Iterators;
import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
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
    public String getName(){
        return basePartition.getName();
    }

    @Override
    public void close() throws IOException{
        basePartition.close();
    }

    @Override
    public Object get(Object o) throws IOException{
        return basePartition.get(o);
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
    public CloseableIterator scan(Object o) throws IOException{
        return basePartition.scan(o);
    }

    @Override
    public DataScanner openScanner(DataScan scan) throws IOException{
        return openScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataScanner openScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        txnReadController.preProcessScan(scan);
        TxnView txnView=txnOpFactory.fromReads(scan);
        if(txnView!=null){
            TxnFilter txnFilter=getFilter(scan,txnView);
            scan.filter(new MTxnFilterWrapper(txnFilter)); //TODO -sf- is this the cleanest way to do this?
        }
        return basePartition.openScanner(scan,metricFactory);
    }

    @Override
    public void put(Object o) throws IOException{
        transactor.processPut(basePartition,rollForward,o);
    }

    @Override
    public void put(Object o,Lock rowLock) throws IOException{
        transactor.processPut(basePartition,rollForward,o);
    }

    @Override
    public void put(Object o,boolean durable) throws IOException{
        transactor.processPut(basePartition,rollForward,o);
    }

    @Override
    public void put(List list) throws IOException{
        transactor.processPutBatch(basePartition,rollForward,list.toArray());
    }

    @Override
    public void put(DataPut put) throws IOException{
        transactor.processPut(basePartition,rollForward,put);
    }

    @Override
    public boolean checkAndPut(byte[] family,byte[] qualifier,byte[] expectedValue,Object o) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void delete(Object o,Lock rowLock) throws IOException{
        throw new UnsupportedOperationException("Deletes are not transactionally supported");
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
    public Lock getLock(byte[] rowKey,boolean waitForLock) throws IOException{
        return basePartition.getLock(rowKey,waitForLock);
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
    public void increment(byte[] rowKey,byte[] family,byte[] qualifier,long amount) throws IOException{
        basePartition.increment(rowKey,family,qualifier,amount);
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
        TxnView txnView=txnOpFactory.fromReads(scan);
        if(txnView!=null){
            TxnFilter txnFilter=getFilter(scan,txnView);
            scan.filter(new MTxnFilterWrapper(txnFilter));
        }
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
    public void mutate(DataMutation put) throws IOException{
        if(put instanceof DataPut)
            put((DataPut)put);
        else delete((DataDelete)put);

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
}
