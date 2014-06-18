package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnAccess;
import com.splicemachine.utils.hash.MurmurHash;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TxnAccess which caches transaction which have "Completed"--i.e. which have entered the COMMITTED or ROLLEDBACK
 * state.
 *
 * This class is thread-safe, and safe to be shared between many threads.
 *
 * @author Scott Fines
 * Date: 6/18/14
 */
public class CompletedTxnCacheAccess implements TxnAccess {

		private Segment[] segments;
		private final int maxSize;

		private final TxnAccess delegate;

		@SuppressWarnings("unchecked")
		public CompletedTxnCacheAccess(TxnAccess delegate, int maxSize, int concurrencyLevel) {
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

		@Override
		public Txn getTransaction(long txnId) throws IOException {
				int hash = MurmurHash.murmur3_32(txnId,0);

				int pos = hash & (segments.length-1); //find the lock for this hash
				Txn txn = segments[pos].get(txnId);
				if(txn!=null) return txn;
				//bummer, we aren't in the cache, need to check the delegate
				Txn transaction = delegate.getTransaction(txnId);
				switch(transaction.getState()){
						case COMMITTED:
						case ROLLEDBACK:
								segments[pos].put(transaction); //it's been completed, so cache it for future use
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

		private static class Segment {
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
										position = nextEvictPosition; //evict the first entry
										nextEvictPosition= (nextEvictPosition+1) & (data.length-1);
								} else
									size++;
								data[position] = new SoftReference<Txn>(txn);
								return true;
						}finally{
								writeLock.unlock();
						}
				}
		}

}
