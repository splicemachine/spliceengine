package com.splicemachine.si.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.readresolve.AsyncReadResolver;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.rollforward.RollForwardManagement;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.si.impl.rollforward.SegmentedRollForward;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Date: 7/2/14
 */
public class TransactionalRegions {
		private TransactionalRegions(){} //can't make a utility class

		private static final Object lock = new Integer("3");
		private static volatile AsyncReadResolver readResolver;

		private static final ScheduledExecutorService rollForwardScheduler;
    private static final ConcurrentMap<String,DiscardingTransactionalRegion> regionMap = new ConcurrentHashMap<String, DiscardingTransactionalRegion>();
    private static volatile ActionFactory actionFactory = ActionFactory.NOOP_ACTION_FACTORY;

    private static final RollForwardStatus status = new RollForwardStatus();

		static{
				ThreadFactory rollForwardFactory = new ThreadFactoryBuilder().setDaemon(true)
								.setNameFormat("roll-forward-scheduler-%d").build();
				rollForwardScheduler = Executors.newScheduledThreadPool(4,rollForwardFactory);
		}

    public static RollForwardManagement getRollForwardManagement(){ return status; }

    public static void setActionFactory(ActionFactory factory) { actionFactory = factory; }

    public static TransactionalRegion get(HRegion region){
        String regionNameAsString = region.getRegionNameAsString();
        DiscardingTransactionalRegion txnRegion = regionMap.get(regionNameAsString);
        if(txnRegion==null){
            txnRegion = new DiscardingTransactionalRegion(get(region,actionFactory.newAction(region)),regionNameAsString);
            DiscardingTransactionalRegion old = regionMap.putIfAbsent(regionNameAsString, txnRegion);
            if(old!=null)
                txnRegion = old;
        }
        txnRegion.addReference();
        return txnRegion;
    }

    public static TransactionalRegion nonTransactionalRegion(HRegion region){
        return new TxnRegion(region,
                NoopRollForward.INSTANCE,
                NoOpReadResolver.INSTANCE,
                TransactionStorage.getTxnSupplier(),
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
				return new SegmentedRollForward(region,rollForwardScheduler,numSegments,
                rollForwardRowThreshold,rollForwardTxnThreshold,rollForwardAction,status);
		}

		private static AsyncReadResolver initializeReadResolver() {
				synchronized (lock){
						AsyncReadResolver arr = readResolver;
						if(arr==null){
								arr = new AsyncReadResolver(4,1<<16,TransactionStorage.getTxnSupplier(),status); //TODO -sf- move these to constants
								arr.start();
								readResolver = arr;
						}
						return arr;
				}
		}

    public static RollForwardStatus getRollForwardStatus() {
        return status;
    }


    private static class DiscardingTransactionalRegion implements TransactionalRegion{
        private final TransactionalRegion delegate;
        private final String name;
        private final AtomicInteger referenceCount = new AtomicInteger(0);

        private DiscardingTransactionalRegion(TransactionalRegion delegate, String name) {
            this.delegate = delegate;
            this.name = name;
            referenceCount.incrementAndGet();
        }

        public void addReference(){
            referenceCount.incrementAndGet();
        }

        @Override
        public TxnFilter unpackedFilter(TxnView txn) throws IOException {
            return delegate.unpackedFilter(txn);
        }

        @Override
        public TxnFilter packedFilter(TxnView txn, EntryPredicateFilter predicateFilter, boolean countStar) throws IOException {
            return delegate.packedFilter(txn, predicateFilter, countStar);
        }

        @Override public DDLFilter ddlFilter(Txn ddlTxn) throws IOException { return delegate.ddlFilter(ddlTxn); }
        @Override public SICompactionState compactionFilter() throws IOException { return delegate.compactionFilter(); }
        @Override public boolean isClosed() { return delegate.isClosed(); }
        @Override public boolean rowInRange(byte[] row) { return delegate.rowInRange(row); }
        @Override public boolean containsRange(byte[] start, byte[] stop) { return delegate.containsRange(start, stop); }
        @Override public String getTableName() { return delegate.getTableName(); }
        @Override public void updateWriteRequests(long writeRequests) { delegate.updateWriteRequests(writeRequests); }
        @Override public void updateReadRequests(long readRequests) { delegate.updateReadRequests(readRequests); }

        @Override
        public OperationStatus[] bulkWrite(TxnView txn, byte[] family, byte[] qualifier, ConstraintChecker constraintChecker, Collection<KVPair> data) throws IOException {
            return delegate.bulkWrite(txn, family, qualifier, constraintChecker, data);
        }

        @Override public String getRegionName() { return delegate.getRegionName(); }
        @Override public TxnSupplier getTxnSupplier() { return delegate.getTxnSupplier(); }
        @Override public ReadResolver getReadResolver() { return delegate.getReadResolver(); }
        @Override public DataStore getDataStore() { return delegate.getDataStore(); }

        @Override
        public void discard() {
            /*
             * This isn't perfectly thread-safe. It's possible that someone
             * could come in and increment the reference count after we decrement
             * the reference count. In this case, we will have removed the entry
             * from the map and discarded the underlying delegate, but we won't
             * have in the other thread.
             *
             * While this is a problem, in practice, we only discard() when a region
             * closes, so there's no real reason to expect that this happens in real-life.
             * Further, we know that the delegate doesn't actually need to discard anything,
             * so the consequences is just that someone will potentially remove entries from
             * the cache too soon. Oh well
             */
            int count = referenceCount.decrementAndGet();
            if(count==0){
                delegate.discard();
                //remove from cache
                regionMap.remove(name);
            }
        }

        @Override
        public InternalScanner compactionScanner(InternalScanner scanner) {
            return delegate.compactionScanner(scanner);
        }
    }

}
