package com.splicemachine.si.impl;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.si.api.SIConfigurations;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplier;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Factory class for obtaining a transaction store, Transaction supplier, etc.
 *
 * @author Scott Fines
 *         Date: 7/3/14
 */
@SuppressFBWarnings("DM_NUMBER_CTOR") //for some reason, FindBug ignores this when it's on a field
public class TransactionStorage{
    //the boxing is actually necessary, regardless of what the compiler says
    private static final Object lock=new Integer("1");
    /*
     * The Base Txn Store for use. This store does NOT necessarily
     * cache any transactions of any kind, and callers should be aware of that.
     * If they want a caching Txn supplier, then consider using the
     * cachedTransactionSupplier instead.
     */
    private static volatile
    @ThreadSafe
    TxnStore baseStore;

    private static volatile
    @ThreadSafe
    CompletedTxnCacheSupplier cachedTransactionSupplier;

    private static volatile IgnoreTxnCacheSupplier ignoreTxnCacheSupplier;

    private static volatile TxnStoreManagement storeManagement;

    public static TxnSupplier getTxnSupplier(){
        TxnSupplier supply=cachedTransactionSupplier;
        //only do 2 volatile reads the very first few calls
        if(supply!=null) return supply;

        lazyInitialize();
        return cachedTransactionSupplier;
    }

    public static IgnoreTxnCacheSupplier getIgnoreTxnSupplier(){
        IgnoreTxnCacheSupplier supply=ignoreTxnCacheSupplier;
        //only do 2 volatile reads the very first few calls
        if(supply!=null) return supply;

        lazyInitialize();
        return ignoreTxnCacheSupplier;
    }

    public static @ThreadSafe TxnStore getTxnStore(){
        TxnStore store=baseStore;
        if(store!=null) return store;

        //initialize, then return
        lazyInitialize();
        return baseStore;
    }

    /*
     * Useful primarily for testing
     */
    @SuppressFBWarnings("DL_SYNCHRONIZATION_ON_UNSHARED_BOXED_PRIMITIVE")
    public static void setTxnStore(@ThreadSafe TxnStore store){
        synchronized(lock){
            baseStore=store;
            SConfiguration config = SIDriver.getConfiguration();
            cachedTransactionSupplier=new CompletedTxnCacheSupplier(baseStore,
                    config.getInt(SIConfigurations.completedTxnCacheSize),
                    config.getInt(SIConfigurations.completedTxnConcurrency));
            storeManagement=new TxnStoreManagement();
            ignoreTxnCacheSupplier=new IgnoreTxnCacheSupplier(SIDriver.getDataLib());
        }
    }

    @SuppressFBWarnings("DL_SYNCHRONIZATION_ON_UNSHARED_BOXED_PRIMITIVE")
    private static void lazyInitialize(){
        /*
		 * We use this to initialize our transaction stores
		 */
        SConfiguration config = SIDriver.getConfiguration();
        synchronized(lock){
            if(baseStore==null){
                TxnStore txnStore;
                try{
                    txnStore=SIDriver.getTxnStore();
                }catch(Exception e){
                    throw new RuntimeException("Cannot establish connection",e);
                }

                if(cachedTransactionSupplier==null){
                    cachedTransactionSupplier=new CompletedTxnCacheSupplier(txnStore,
                            config.getInt(SIConfigurations.completedTxnCacheSize),
                            config.getInt(SIConfigurations.completedTxnConcurrency));
                }
                txnStore.setCache(cachedTransactionSupplier);
                baseStore=txnStore;
            }else if(cachedTransactionSupplier==null){
                cachedTransactionSupplier=new CompletedTxnCacheSupplier(baseStore,
                        config.getInt(SIConfigurations.completedTxnCacheSize),
                        config.getInt(SIConfigurations.completedTxnConcurrency));
            }
            storeManagement=new TxnStoreManagement();

            if(ignoreTxnCacheSupplier==null)
                ignoreTxnCacheSupplier=new IgnoreTxnCacheSupplier(SIDriver.getDataLib());
        }
    }

    public static TxnStoreManagement getTxnStoreManagement(){
        TxnStoreManagement store=storeManagement;
        if(store==null){
            lazyInitialize();
            store=storeManagement;
        }
        return store;
    }

    private static class TxnStoreManagement implements com.splicemachine.si.api.txn.TxnStoreManagement{
        @Override public long getTotalTxnLookups(){ return baseStore.lookupCount(); }
        @Override public long getTotalTxnElevations(){ return baseStore.elevationCount(); }
        @Override public long getTotalWritableTxnsCreated(){ return baseStore.createdCount(); }
        @Override public long getTotalRollbacks(){ return baseStore.rollbackCount(); }
        @Override public long getTotalCommits(){ return baseStore.commitCount(); }
        @Override public long getTotalEvictedCacheEntries(){ return cachedTransactionSupplier.getTotalEvictedEntries(); }
        @Override public long getTotalCacheHits(){ return cachedTransactionSupplier.getTotalHits(); }
        @Override public long getTotalCacheMisses(){ return cachedTransactionSupplier.getTotalMisses(); }
        @Override public long getTotalCacheRequests(){ return cachedTransactionSupplier.getTotalRequests(); }
        @Override public float getCacheHitPercentage(){ return cachedTransactionSupplier.getHitPercentage(); }
        @Override public int getCurrentCacheSize(){ return cachedTransactionSupplier.getCurrentSize(); }
        @Override public int getMaxCacheSize(){ return cachedTransactionSupplier.getMaxSize(); }
    }
}
