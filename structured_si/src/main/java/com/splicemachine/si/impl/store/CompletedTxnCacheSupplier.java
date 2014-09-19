package com.splicemachine.si.impl.store;

import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import org.cliffc.high_scale_lib.Counter;
import com.splicemachine.si.api.TxnView;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TxnSupplier which caches transaction which have "Completed"--i.e. which have entered the COMMITTED or ROLLEDBACK
 * state.
 *
 * This class is thread-safe, and safe to be shared between many threads.
 *
 * @author Scott Fines
 * Date: 6/18/14
 */
public class CompletedTxnCacheSupplier implements TxnSupplier {

		private Segment[] segments;
		private final int maxSize;

		private final TxnSupplier delegate;

		private final AtomicLong hits = new AtomicLong();
		private final AtomicLong requests = new AtomicLong();
		private final AtomicLong evicted = new AtomicLong();

    private final Hash32 hashFunction = HashFunctions.murmur3(0);

		@SuppressWarnings("unchecked")
		public CompletedTxnCacheSupplier(TxnSupplier delegate, int maxSize, int concurrencyLevel) {
				this.maxSize = maxSize;
				int s =1;
				while(s<=maxSize){
						s<<=1;
				}
				s<<=1; //make it twice as big to keep the load factor low
				int c = 1;
				while(c<concurrencyLevel){
						c<<=1;
				}
				concurrencyLevel = c;
				this.segments = new Segment[concurrencyLevel];
				int segmentSize = s / concurrencyLevel;
				for(int i=0;i<concurrencyLevel;i++){
						segments[i] = new Segment(segmentSize);
				}
				this.delegate = delegate;
		}

		public int getCurrentSize() {
        int totalSize = 0;
        for(Segment segment:segments){
            totalSize+=segment.size;
        }
        return totalSize;
    }

		public int getMaxSize() { return maxSize*segments.length; }
		public long getTotalHits() { return hits.get(); }
		public long getTotalRequests() { return requests.get(); }
		public long getTotalMisses() { return getTotalRequests()-getTotalHits(); }

		public float getHitPercentage() {
				long totalRequests = getTotalRequests();
				long hits = getTotalHits();
				return (float)(((double)hits)/totalRequests);
		}

		public long getTotalEvictedEntries() { return evicted.get(); }

		@Override
		public TxnView getTransaction(long txnId) throws IOException {
        if(txnId==-1) return Txn.ROOT_TRANSACTION;
				return getTransaction(txnId,false);
		}

		@Override
		public TxnView getTransaction(long txnId, boolean getDestinationTables) throws IOException {
        if(txnId==-1) return Txn.ROOT_TRANSACTION;
				requests.incrementAndGet();
				int hash = hashFunction.hash(txnId);

        int pos = hash & (segments.length-1); //find the lock for this hash
        TxnView txn = segments[pos].get(txnId);
        if(txn!=null){
            hits.incrementAndGet();
            return txn;
        }
        //bummer, we aren't in the cache, need to check the delegate
        TxnView transaction = delegate.getTransaction(txnId,getDestinationTables);
        if(transaction==null) //noinspection ConstantConditions
            return transaction; //don't cache read-only transactions;

				switch(transaction.getEffectiveState()){
						case COMMITTED:
						case ROLLEDBACK:
								segments[pos].put(transaction); //it's been completed, so cache it for future use
				}
				return transaction;
		}

		@Override
		public boolean transactionCached(long txnId) {
				int hash = hashFunction.hash(txnId);

				int pos = hash & (segments.length-1); //find the lock for this hash
				TxnView txn = segments[pos].get(txnId);
				return txn !=null;
		}

		@Override
		public void cache(TxnView toCache) {
				if(toCache.getState()== Txn.State.ACTIVE) return; //cannot cache incomplete transactions
				long txnId = toCache.getTxnId();
				int hash = hashFunction.hash(txnId);

				int pos = hash & (segments.length-1); //find the lock for this hash
				segments[pos].put(toCache);
		}

    @Override
    public TxnView getTransactionFromCache(long txnId) {
        requests.incrementAndGet();
        int hash =hashFunction.hash(txnId);

        int pos = hash & (segments.length-1); //find the lock for this hash
        TxnView txn = segments[pos].get(txnId);
        if(txn!=null)
            hits.incrementAndGet();
        return txn;
    }

    private class Segment {
				private final Lock readLock;
        private final Lock writeLock;
				private volatile int size = 0;

        private SoftReference<TxnView>[] data;

				@SuppressWarnings("unchecked")
				private Segment(int maxSize) {
            this.readLock = writeLock = new ReentrantLock(false);
						int s = 1;
						while(s<=maxSize){
								s<<=1;
						}
						s<<=1;
						this.data = new SoftReference[s<<1]; //double the size to avoid hash collisions
				}

				TxnView get(long txnId){
						readLock.lock();
						try{
                int pos = hashFunction.hash(txnId) & (data.length-1);
                int s = size;
								for(int i=0;i<s;i++){
										SoftReference<TxnView> datum = data[pos];
										if(datum==null) continue;
										TxnView v = datum.get();
										if(v==null){
                        //evicted due to memory pressure
                        size--;
                        continue;
                    }
										if(v.getTxnId()==txnId) return v;
                    pos = (pos+1)& (data.length-1);
								}
								return null;
						}finally{
								readLock.unlock();
						}
				}

				boolean put(TxnView txn){
						writeLock.lock();
						try{
                int pos = hashFunction.hash(txn.getTxnId()) & (data.length-1);
                if(size>=maxSize){
                    /*
                     * We are larger than the total allowed size of the cache. To
                     * ensure that we stay below that size, we evict an entry.
                     *
                     * However, we don't want to waste a lot of time evicting entries
                     * if we don't REALLY have to. Basically, we hash this entry
                     * into a position. If that position is occupied, we evict
                     * that entry. Otherwise, we fill that entry and move on. This way
                     * we do only a single step, at the cost of randomly evicting entries.
                     */
                    SoftReference<TxnView> datum = data[pos];
                    if(datum!=null){
                        TxnView present = datum.get();
                        if(present!=null){
                            if(txn.equals(present)) return true;
                            evicted.incrementAndGet();
                            size--;
                        }
                    }
                }else{
                    /*
                     * We are below the maximum size, so just use linear probing
                     * to find the next open slot.
                     */
                    for(int i=0;i<size;i++){
                        SoftReference<TxnView> datum = data[pos];
                        if(datum==null||datum.get()==null){
                            break;
                        }else
                            pos = (pos+1)&(data.length-1);
                    }
                }
								data[pos] = new SoftReference<TxnView>(txn);
                size++;
								return true;
						}finally{
								writeLock.unlock();
						}
				}
		}

}
