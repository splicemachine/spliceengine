package com.splicemachine.si.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.RollForward;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.impl.readresolve.AsyncReadResolver;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.rollforward.RollForwardManagement;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.si.impl.rollforward.SegmentedRollForward;
import com.splicemachine.utils.GreenLight;
import com.splicemachine.utils.TrafficControl;
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
    private static volatile ActionFactory actionFactory = ActionFactory.NOOP_ACTION_FACTORY;
    private static volatile TrafficControl trafficControl = GreenLight.INSTANCE;

    private static final RollForwardStatus status = new RollForwardStatus();

		static{
				ThreadFactory rollForwardFactory = new ThreadFactoryBuilder().setDaemon(true)
								.setNameFormat("roll-forward-scheduler-%d").build();
				rollForwardScheduler = Executors.newScheduledThreadPool(4,rollForwardFactory);
    }

    public static RollForwardManagement getRollForwardManagement(){ return status; }

    public static void setActionFactory(ActionFactory factory) { actionFactory = factory; }

    public static void setTrafficControl(TrafficControl control) {trafficControl = control;}

    public static TransactionalRegion get(HRegion region){
        return get(region,actionFactory.newAction(region));
    }

    public static TransactionalRegion nonTransactionalRegion(HRegion region){
        return new TxnRegion(region,
                NoopRollForward.INSTANCE,
                NoOpReadResolver.INSTANCE,
                TransactionStorage.getTxnSupplier(),
                TransactionStorage.getIgnoreTxnSupplier(),
                TxnDataStore.getDataStore(),
                HTransactorFactory.getTransactor());
    }

    /*private helper methods*/
    private static TransactionalRegion get(HRegion region,SegmentedRollForward.Action rollForwardAction){

        RollForward rollForward = getRollForward(region, rollForwardAction);
        ReadResolver resolver = getReadResolver(region, rollForward);
        return new TxnRegion(region,
                rollForward,
                resolver,
                TransactionStorage.getTxnSupplier(),
                TransactionStorage.getIgnoreTxnSupplier(),
                TxnDataStore.getDataStore(),
                HTransactorFactory.getTransactor());
    }

		private static ReadResolver getReadResolver(HRegion region,RollForward rollForward) {
				AsyncReadResolver arr = readResolver;
				if(arr==null)
						arr = initializeReadResolver();

				return arr.getResolver(region,rollForward);
		}

		private static RollForward getRollForward(HRegion region, SegmentedRollForward.Action rollForwardAction){
        if(rollForwardAction==SegmentedRollForward.NOOP_ACTION)
            return NoopRollForward.INSTANCE;

        int numSegments = SpliceConstants.numRollForwardSegments;
        int rollForwardRowThreshold = SpliceConstants.rollForwardRowThreshold;
        int rollForwardTxnThreshold = SpliceConstants.rollForwardTxnThreshold;
        long rollForwardForceInterval = SpliceConstants.rollForwardInterval;
				return new SegmentedRollForward(region,rollForwardScheduler,numSegments,
                rollForwardRowThreshold,rollForwardTxnThreshold,rollForwardForceInterval,rollForwardAction,status);
		}

		private static AsyncReadResolver initializeReadResolver() {
				synchronized (lock){
						AsyncReadResolver arr = readResolver;
						if(arr==null){
								arr = new AsyncReadResolver(SIConstants.readResolverThreads,
                        SIConstants.readResolverQueueSize,
                        TransactionStorage.getTxnSupplier(),
                        status,trafficControl);
								arr.start();
								readResolver = arr;
						}
						return arr;
				}
		}

    public static RollForwardStatus getRollForwardStatus() {
        return status;
    }


}
