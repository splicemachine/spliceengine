package com.splicemachine.si.api;

import com.splicemachine.si.impl.RollForwardFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Pair;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Entity for obtaining transaction resources for an individual region.
 *
 * @author Scott Fines
 * Date: 6/27/14
 */
public class TransactionResources {

		private static final ConcurrentMap<String,Pair<TransactionalWriter,AtomicInteger>> transactionalWriters = new NonBlockingHashMap<String, Pair<TransactionalWriter, AtomicInteger>>();
		private static final RollForwardFactory rollForwardFactory = new RollForwardFactory();

		public static TransactionalWriter getRegionWriter(HRegion region){
				Pair<TransactionalWriter,AtomicInteger> txnPair = transactionalWriters.get(region.getRegionNameAsString());
				if(txnPair==null)
						txnPair = buildTransactionalWriter(region);
				else{
						txnPair.getSecond().incrementAndGet(); //add to the reference counter
				}
				return txnPair.getFirst();
		}

		public static void returnRegionWriter(HRegion region){
				Pair<TransactionalWriter,AtomicInteger> txnPair = transactionalWriters.get(region.getRegionNameAsString());
				if(txnPair!=null){
						int i = txnPair.getSecond().decrementAndGet();
						if(i<=0)
								transactionalWriters.remove(region.getRegionNameAsString(),txnPair);
				}
		}

		public static Pair<TransactionalWriter,AtomicInteger> buildTransactionalWriter(HRegion region){
			throw new UnsupportedOperationException("IMPLEMENT");
		}
}
