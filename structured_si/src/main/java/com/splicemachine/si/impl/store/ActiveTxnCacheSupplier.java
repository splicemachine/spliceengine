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
    private final Hash32 hashFunction;

		public ActiveTxnCacheSupplier(TxnSupplier delegate, int maxSize) {
				int s = 1;
				while(s<maxSize){
						s<<=1;
				}
				s<<=1;
				//noinspection unchecked
				data = new SoftReference[s<<1]; //double the size to avoid hash collisions
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

				if(txn.getEffectiveState()== Txn.State.ACTIVE)
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
//						delegate.cache(toCache);
//				}else{
						addToCache(hashFunction.hash(toCache.getTxnId()), toCache);
//		}
		}

    @Override
    public TxnView getTransactionFromCache(long txnId) {
        int hash = hashFunction.hash(txnId);
        return getFromCache(hash,txnId);
    }

    public int getSize(){
        return size;
    }

    protected void addToCache(int hash, TxnView txn) {
				int pos = hash &(data.length-1) ;
				//cache it for future use
				if(size>=maxSize){
            /*
             * Since we are larger than the "maxSize", we will
             * try and evict an entry if we come to it. However,
             * if we find an empty slot first, we will just fill
             * that slot, which will allow us to grow beyond
             * the max size allotted (although not larger than
             * the total size of the array).
             *
             * Also, this requires no looping, so once the
             * cache is full, we are very efficient. However,this
             * also pays no attention to liveness--adding a new
             * element could remove a frequently accessed entry.
             */
            SoftReference<TxnView> elem = data[pos];
//            if(elem!=null && elem.get()==null)
                size--;
        }else{
						//find the next empty spot
						for(int i=0;i<size;i++){
								if(data[pos]==null || data[pos].get()==null){
                    break;
								}else
                    pos = (pos+1) &(data.length-1);
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
