package com.splicemachine.si.impl.store;

import com.splicemachine.si.api.TransactionCacheManagement;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import org.cliffc.high_scale_lib.Counter;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

		private final AtomicInteger size = new AtomicInteger();
		private final AtomicLong hits = new AtomicLong();
		private final AtomicLong requests = new AtomicLong();
		private final AtomicLong evicted = new AtomicLong();

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

		@Override public int getCurrentSize() { return size.get(); }
		@Override public int getMaxSize() { return maxSize; }
		@Override public long getTotalHits() { return hits.get(); }
		@Override public long getTotalRequests() { return requests.get(); }
		@Override public long getTotalMisses() { return getTotalRequests()-getTotalHits(); }

		@Override
		public float getHitPercentage() {
				long totalRequests = getTotalRequests();
				long hits = getTotalHits();
				return (float)(((double)hits)/totalRequests);
		}

		@Override public long getTotalEvictedEntries() { return evicted.get(); }

		@Override
		public Txn getTransaction(long txnId) throws IOException {
				return getTransaction(txnId,false);
		}

		@Override
		public Txn getTransaction(long txnId, boolean getDestinationTables) throws IOException {
				requests.incrementAndGet();
				int hash = MurmurHash.murmur3_32(txnId,0);

        int pos = hash & (segments.length-1); //find the lock for this hash
        Txn txn = segments[pos].get(txnId);
        if(txn!=null){
            hits.incrementAndGet();
            return txn;
        }
        //bummer, we aren't in the cache, need to check the delegate
        Txn transaction = delegate.getTransaction(txnId);
        if(transaction==null) //noinspection ConstantConditions
            return transaction; //don't cache read-only transactions;

				switch(transaction.getState()){
						case COMMITTED:
						case ROLLEDBACK:
								segments[pos].put(transaction); //it's been completed, so cache it for future use
								size.incrementAndGet();
				}
				return transaction;
		}

		@Override
		public boolean transactionCached(long txnId) {
				int hash = MurmurHash.murmur3_32(txnId,0);

				int pos = hash & (segments.length-1); //find the lock for this hash
				Txn txn = segments[pos].get(txnId);
				return txn !=null;
		}

		@Override
		public void cache(Txn toCache) {
				if(toCache.getState()== Txn.State.ACTIVE) return; //cannot cache incomplete transactions
				long txnId = toCache.getTxnId();
				int hash = MurmurHash.murmur3_32(txnId,0);

				int pos = hash & (segments.length-1); //find the lock for this hash
				segments[pos].put(toCache);
		}

    @Override
    public Txn getTransactionFromCache(long txnId) {
        requests.incrementAndGet();
        int hash = MurmurHash.murmur3_32(txnId,0);

        int pos = hash & (segments.length-1); //find the lock for this hash
        Txn txn = segments[pos].get(txnId);
        if(txn!=null)
            hits.incrementAndGet();
        return txn;
    }

    private class Segment {
				private final ReentrantReadWriteLock lock;
				private volatile int size = 0;
				private int nextEvictPosition = 0;

				private SoftReference<Txn>[] data;

				@SuppressWarnings("unchecked")
				private Segment(int maxSize) {
						lock = new ReentrantReadWriteLock(false);
						int s = 1;
						while(s<=maxSize){
								s<<=1;
						}
						s<<=1;
						this.data = new SoftReference[s];
				}

				Txn get(long txnId){
						ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
						readLock.lock();
						try{
								for(int i=0;i<size;i++){
										SoftReference<Txn> datum = data[i];
										if(datum==null) continue;
										Txn v = datum.get();
										if(v==null) continue;
										if(v.getTxnId()==txnId) return v;
								}
								return null;
						}finally{
								readLock.unlock();
						}
				}

				boolean put(Txn txn){
					ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
						writeLock.lock();
						try{
								int position = -1;
								for(int i=0;i<size;i++){
										SoftReference<Txn> datum = data[i];
										if(datum==null){
												if(position<0)
														position = i;
												continue;
										}
										Txn v = datum.get();
										if(v==null){
												if(position<0)
														position = i;
												continue;
										}
										if(v.getTxnId()==txn.getTxnId()){
												//another thread filled in the entry--bad luck
												return false;
										}
								}
								if(position<0 && size==data.length){
										evicted.incrementAndGet();
										position = nextEvictPosition; //evict the first entry
										nextEvictPosition= (nextEvictPosition+1) & (data.length-1);
								} else{
										position = size;
										size++;
								}
								data[position] = new SoftReference<Txn>(txn);
								return true;
						}finally{
								writeLock.unlock();
						}
				}
		}

}
