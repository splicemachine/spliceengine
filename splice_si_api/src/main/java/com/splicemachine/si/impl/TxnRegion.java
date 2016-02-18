package com.splicemachine.si.impl;

import com.google.common.collect.Iterators;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.filter.HRowAccumulator;
import com.splicemachine.si.impl.filter.PackedTxnFilter;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;

/**
 * Base implementation of a TransactionalRegion
 *
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class TxnRegion<InternalScanner> implements TransactionalRegion<InternalScanner>{
    private final RollForward rollForward;
    private final ReadResolver readResolver;
    private final TxnSupplier txnSupplier;
    private final IgnoreTxnCacheSupplier ignoreTxnCacheSupplier;
    private final Transactor transactor;
    private final TxnOperationFactory opFactory;
    private Partition region;
    private String tableName;


    public TxnRegion(Partition region,
                     RollForward rollForward,
                     ReadResolver readResolver,
                     TxnSupplier txnSupplier,
                     IgnoreTxnCacheSupplier ignoreTxnCacheSupplier,
                     Transactor transactor,TxnOperationFactory opFactory){
        this.region=region;
        this.rollForward=rollForward;
        this.readResolver=readResolver;
        this.txnSupplier=txnSupplier;
        this.ignoreTxnCacheSupplier=ignoreTxnCacheSupplier;
        this.transactor=transactor;
        this.opFactory=opFactory;
        if(region!=null){
            this.tableName=region.getTableName();
        }
    }

    @Override
    public TxnFilter unpackedFilter(TxnView txn) throws IOException{
        return new SimpleTxnFilter(tableName,txn,readResolver,txnSupplier,ignoreTxnCacheSupplier);
    }

    @Override
    public TxnFilter packedFilter(TxnView txn,EntryPredicateFilter predicateFilter,boolean countStar) throws IOException{
        return new PackedTxnFilter(unpackedFilter(txn),new HRowAccumulator(predicateFilter,new EntryDecoder(),countStar));
    }

//    @Override
//    public SICompactionState compactionFilter() throws IOException{
//        throw new UnsupportedOperationException("IMPLEMENT");
//    }

    @Override
    public InternalScanner compactionScanner(InternalScanner internalScanner){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean rowInRange(byte[] row){
        return region.containsRow(row);
    }

    @Override
    public boolean rowInRange(ByteSlice slice){
        return region.containsRow(slice.array(),slice.offset(),slice.length());
    }

    @Override
    public boolean isClosed(){
        return region.isClosed() || region.isClosing();
    }

    @Override
    public String getTableName(){
        return tableName;
    }

    @Override
    public void updateWriteRequests(long writeRequests){
        region.writesRequested(writeRequests);
    }

    @Override
    public Iterable<MutationStatus> bulkWrite(TxnView txn,
                                       byte[] family,byte[] qualifier,
                                       ConstraintChecker constraintChecker, //TODO -sf- can we encapsulate this as well?
                                       Collection<KVPair> data) throws IOException{
        /*
         * Designed for subclasses. Override this if you want to bypass transactional writes
         */
        final MutationStatus[] status = transactor.processKvBatch(region, rollForward, family, qualifier, data,txn,constraintChecker);
        return new Iterable<MutationStatus>(){
            @Override public Iterator<MutationStatus> iterator(){ return Iterators.forArray(status); }
        };
    }

    @Override
    public boolean verifyForeignKeyReferenceExists(TxnView txnView,byte[] rowKey) throws IOException{
        Lock rowLock=region.getRowLock(rowKey,0,rowKey.length);
        rowLock.lock();
        try{
            //TODO -sf- dg,result, and simpleTxnFilter can all be cached to reduce churn during large FK check batches
            /*
             * We do a non-transactional lookup here, and perform the transaction resolution ourselves. This
             * ensures that we properly perform lookups for the foreign key. In particular, we want to ensure
             * that we get the latest and greatest foreign key counter.
             *
             */
            DataGet dg = opFactory.newDataGet(null,rowKey,null);
            DataResult result=region.get(dg,null);
            //needs to be transactional
            if(result!=null && result.size()>0){
                SimpleTxnFilter simpleTxnFilter=new SimpleTxnFilter(getTableName(),txnView,NoOpReadResolver.INSTANCE,txnSupplier,ignoreTxnCacheSupplier);
                int cellCount = result.size();
                if(result.fkCounter()!=null){
                    //make sure that rows which only have an FK counter are treated as visible.
                    cellCount++;
                }
                for(DataCell dc:result){
                    DataFilter.ReturnCode returnCode=simpleTxnFilter.filterCell(dc);
                    switch(returnCode){
                        case NEXT_ROW:
                            return false; //the entire row is filtered
                        case SKIP:
                        case NEXT_COL:
                            cellCount--;
                            break;
                    }
                }
                if(cellCount<=0) return false;
                transactor.updateCounterColumn(region,txnView,rowKey);
                return true;
            }
        }finally{
            rowLock.unlock();
        }
        return false;
    }

    @Override
    public String getRegionName(){
        return region.getName();
    }

    @Override
    public TxnSupplier getTxnSupplier(){
        return txnSupplier;
    }

    @Override
    public ReadResolver getReadResolver(){
        return readResolver;
    }

    @Override
    public void close(){
    } //no-op

    @Override
    public Partition unwrap(){
        return region;
    }
}
