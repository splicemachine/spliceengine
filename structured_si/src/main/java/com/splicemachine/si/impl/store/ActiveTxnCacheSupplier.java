package com.splicemachine.si.impl.store;

import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;

import java.io.IOException;
import java.lang.ref.SoftReference;

/**
 * Represents a Transaction Store which caches active transactions. This is intended for scans,
 * where a transaction may only be cached for a particular operation.
 *
 * This class is <em>not</em> thread-safe--only a single thread may use this at a time without
 * external synchronization. However, as a single transaction should be represented using a single thread
 * anyway, this class doesn't make sense to be thread safe anyway.
 *
 * @author Scott Fines
 * Date: 6/18/14
 */
public class ActiveTxnCacheSupplier implements TxnSupplier {
		private final SoftReference<TxnView>[] data;
		private int size;
		private final int maxSize;
		private final TxnSupplier delegate;
		private boolean cacheGlobally;
    private final Hash32 hashFunction;

		public ActiveTxnCacheSupplier(TxnSupplier delegate, int maxSize){
				this(delegate, maxSize,false);
		}
		public ActiveTxnCacheSupplier(TxnSupplier delegate, int maxSize, boolean cacheGlobally) {
				int s = 1;
				while(s<maxSize){
						s<<=1;
				}
				s<<=1;
				//noinspection unchecked
				data = new SoftReference[s];
				this.delegate = delegate;
				this.maxSize = maxSize;
        this.hashFunction = HashFunctions.murmur3(0);
    }

		@Override
		public TxnView getTransaction(long txnId) throws IOException {
				return getTransaction(txnId,false);
		}

		@Override
		public TxnView getTransaction(long txnId, boolean getDestinationTables) throws IOException {
				int hash = hashFunction.hash(txnId);
				TxnView txn = getFromCache(hash,txnId);
				if(txn!=null) return txn;
				//bummer, not cached. try delegate
				txn = delegate.getTransaction(txnId,getDestinationTables);
				if(txn==null) return null;

				if(txn.getState()== Txn.State.ACTIVE)
						addToCache(hash, txn);
				return txn;
		}

		@Override
		public boolean transactionCached(long txnId) {
				int hash = hashFunction.hash(txnId);
				TxnView txn = getFromCache(hash,txnId);
				return txn!=null;
		}

		@Override
		public void cache(TxnView toCache) {
//				if(cacheGlobally){
						delegate.cache(toCache);
//				}else{
						addToCache(hashFunction.hash(toCache.getTxnId()), toCache);
//		}
		}

    @Override
    public TxnView getTransactionFromCache(long txnId) {
        int hash = hashFunction.hash(txnId);
        return getFromCache(hash,txnId);
    }

    protected void addToCache(int hash, TxnView txn) {
				int pos = hash &(data.length-1) ;
				//cache it for future use
				if(size==maxSize){
						//evict the next non-null value at hash
						for(int i=0;i<size;i++){
								if(data[i]==null){
										pos = (pos+1) & (data.length-1);
										continue;
								}
								TxnView toEvict = data[i].get();
								if(toEvict==null){
										size--; //memory purged an entry for us
								}
								break;
						}
				}else{
						//find the next empty spot
						for(int i=0;i<size;i++){
								if(data[i]==null || data[i].get()==null){
										pos = i;
								}
						}
				}
				data[pos] = new SoftReference<TxnView>(txn);
				size++;
		}

		private TxnView getFromCache(int hash,long txnId){
				int pos = hash & (data.length-1);
				for(int i=0;i<size;i++){
						if(data[pos]==null) break; //didn't find it
						TxnView txn = data[pos].get();
						if(txn ==null) {
								size--; //element was purged for memory reasons
								continue;
						}
						if(txn.getTxnId()==txnId) return txn;
						pos= (pos+1) & (data.length-1);
				}
				return null;
		}
}
