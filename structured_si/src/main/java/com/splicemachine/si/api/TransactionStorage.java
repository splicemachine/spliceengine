package com.splicemachine.si.api;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.hbase.table.SpliceHTableFactory;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplier;
import com.splicemachine.si.impl.store.LazyTxnSupplier;
import com.splicemachine.si.impl.txnclient.CoprocessorTxnStore;
import com.splicemachine.utils.ThreadSafe;

/**
 * Factory class for obtaining a transaction store, Transaction supplier, etc.
 *
 * @author Scott Fines
 * Date: 7/3/14
 */
public class TransactionStorage {
		//the boxing is actually necessary, regardless of what the compiler says
		@SuppressWarnings("UnnecessaryBoxing")
		private static final Object lock = new Integer(1);
		/*
		 * The Base Txn Store for use. This store does NOT necessarily
		 * cache any transactions of any kind, and callers should be aware of that.
		 * If they want a caching Txn supplier, then consider using the
		 * cachedTransactionSupplier instead.
		 */
		private static volatile @ThreadSafe TxnStore baseStore;

		/*
		 * A Transaction Supplier for when only reads are needed (e.g. writes to the
		 * store are not required). Instances here should expect to be very efficient.
		 *
		 * Callers should have a reasonable expectation that transactions which are
		 * in a final state may be cached by this supplier. If no caching is desired,
		 * consider accessing the baseStore instead.
		 */
		private static volatile @ThreadSafe TxnSupplier cachedTransactionSupplier;
    private static CompletedTxnCacheSupplier cacheManagement;

    public static TxnSupplier getTxnSupplier(){
				TxnSupplier supply = cachedTransactionSupplier;
				//only do 2 volatile reads the very first few calls
				if(supply!=null) return supply;

				lazyInitialize();
				return cachedTransactionSupplier;
		}

		public static @ThreadSafe TxnStore getTxnStore(){
				TxnStore store = baseStore;
				if(store!=null) return store;

				//initialize, then return
				lazyInitialize();
				return baseStore;
		}

		/*
		 * Useful primarily for testing
		 */
		public static void setTxnStore(@ThreadSafe TxnStore store){
				synchronized (lock){
						baseStore = store;
            CompletedTxnCacheSupplier delegate = new CompletedTxnCacheSupplier(baseStore, SIConstants.activeTransactionCacheSize, 16);
            cachedTransactionSupplier=new LazyTxnSupplier(delegate);
            cacheManagement = delegate;
				}
		}

		private static void lazyInitialize() {
				/*
				 * We use this to initialize our transaction stores
				 */
				synchronized (lock){
						if(baseStore==null){
								TimestampSource tsSource = TransactionTimestamps.getTimestampSource();
								CoprocessorTxnStore txnStore = new CoprocessorTxnStore(new SpliceHTableFactory(true),tsSource,null);
								//TODO -sf- configure these fields separately
								if(cachedTransactionSupplier==null){
                    CompletedTxnCacheSupplier delegate = new CompletedTxnCacheSupplier(txnStore, SIConstants.activeTransactionCacheSize, 16);
                    cachedTransactionSupplier =new LazyTxnSupplier(delegate);
                    cacheManagement = delegate;
                }
								txnStore.setCache(cachedTransactionSupplier);
								baseStore = txnStore;
						}else if (cachedTransactionSupplier==null){
                CompletedTxnCacheSupplier delegate = new CompletedTxnCacheSupplier(baseStore, SIConstants.activeTransactionCacheSize, 16);
                cachedTransactionSupplier =new LazyTxnSupplier(delegate);
                cacheManagement = delegate;
						}
				}
		}

    public static TransactionCacheManagement getTxnManagement() {
        TransactionCacheManagement cache = cacheManagement;
        if(cache==null){
            lazyInitialize();
        }
        return cacheManagement;
    }
}
