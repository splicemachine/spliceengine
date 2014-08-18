package com.splicemachine.si.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.readresolve.AsyncReadResolver;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.rollforward.SegmentedRollForward;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * @author Scott Fines
 * Date: 7/2/14
 */
public class TransactionalRegions {
		private TransactionalRegions(){} //can't make a utility class

		private static final Object lock = new Integer("3");
		private static volatile AsyncReadResolver readResolver;

		private static final ScheduledExecutorService rollForwardScheduler;

		static{
				ThreadFactory rollForwardFactory = new ThreadFactoryBuilder().setDaemon(true)
								.setNameFormat("roll-forward-scheduler-%d").build();
				rollForwardScheduler = Executors.newScheduledThreadPool(4,rollForwardFactory);
		}

		public static TransactionalRegion get(HRegion region,SegmentedRollForward.Action rollForwardAction){

				return new TxnRegion(region,
								getRollForward(region,rollForwardAction),
								getReadResolver(region),
								TransactionStorage.getTxnSupplier(),
								TxnDataStore.getDataStore(),
								TxnDataStore.getDataLib(),
								HTransactorFactory.getTransactor());
		}

    public static TransactionalRegion nonTransactionalRegion(HRegion region){
        return new TxnRegion(region,
                NoopRollForward.INSTANCE,
                NoOpReadResolver.INSTANCE,
                TransactionStorage.getTxnSupplier(),
                TxnDataStore.getDataStore(),
                TxnDataStore.getDataLib(),
                HTransactorFactory.getTransactor());
    }

		private static ReadResolver getReadResolver(HRegion region) {
//        return NoOpReadResolver.INSTANCE;
				AsyncReadResolver arr = readResolver;
				if(arr==null)
						arr = initializeReadResolver();

				return arr.getResolver(region);
		}

		private static RollForward getRollForward(HRegion region, SegmentedRollForward.Action rollForwardAction){
        if(rollForwardAction==SegmentedRollForward.NOOP_ACTION)
            return NoopRollForward.INSTANCE;
				return new SegmentedRollForward(region,rollForwardScheduler,2048,1<<14,1<<14,rollForwardAction);
		}

		private static AsyncReadResolver initializeReadResolver() {
				synchronized (lock){
						AsyncReadResolver arr = readResolver;
						if(arr==null){
								arr = new AsyncReadResolver(4,1<<16,TransactionStorage.getTxnSupplier()); //TODO -sf- move these to constants
								arr.start();
								readResolver = arr;
						}
						return arr;
				}
		}
}
