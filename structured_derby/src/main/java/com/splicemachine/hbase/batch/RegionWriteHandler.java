package com.splicemachine.hbase.batch;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.PutToRun;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.data.hbase.HRowLock;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.SITransactor;
import com.splicemachine.tools.ResettableCountDownLatch;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public class RegionWriteHandler implements WriteHandler {
    static final Logger LOG = Logger.getLogger(RegionWriteHandler.class);

    private final HRegion region;
    private final List<Pair<Mutation,Integer>> mutations = Lists.newArrayList();
    private final ResettableCountDownLatch writeLatch;
    private final int writeBatchSize;

    public RegionWriteHandler(HRegion region, ResettableCountDownLatch writeLatch, int writeBatchSize) {
        this.region = region;
        this.writeLatch = writeLatch;
        this.writeBatchSize = writeBatchSize;
    }

    @Override
    public void next(Mutation mutation, WriteContext ctx) {
        /*
         * Write-wise, we are at the end of the line, so make sure that we don't run through
         * another write-pipeline when the Region actually does it's writing
         */
        mutation.setAttribute(IndexSet.INDEX_UPDATED,IndexSet.INDEX_ALREADY_UPDATED);
        if (HRegion.rowIsInRange(ctx.getRegion().getRegionInfo(), mutation.getRow())) {
        	mutations.add(new Pair<Mutation,Integer> (mutation,null));
        } 
        else {
        	ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, "WrongRegion"));
        }
    }

    @SuppressWarnings({ "unchecked", "unused" })
	@Override
    public void finishWrites(final WriteContext ctx) throws IOException {
        /*
         * We have to block here in case someone did a table manipulation under us.
         * If they didn't, then the writeLatch will be exhausted, and I'll be able to
         * go through without problems. Otherwise, I'll have to block until the metadata
         * manipulation is over before proceeding with my writes.
         */
        try {
            writeLatch.await();
        } catch (InterruptedException e) {
            //we've been interrupted! That's a problem, but what to do?
            //we'll have to fail everything, and rely on the system to retry appropriately
            //we can do that easily by just blowing up here
            throw new IOException(e);
        }
        //write all the puts first, since they are more likely
        boolean failed= false;
        Pair<Mutation,Integer>[] toProcess = null;
        List<Pair<Mutation,Integer>> toProcessList = Lists.newArrayListWithCapacity(writeBatchSize);
        try{
            Collection<Pair<Mutation,Integer>> filteredMutations = Collections2.filter(mutations,new Predicate<Pair<Mutation,Integer>>() {
                @Override
                public boolean apply(@Nullable Pair<Mutation,Integer> input) {
                    return ctx.canRun(input);
                }
            });

            for(Pair<Mutation,Integer> mutation:filteredMutations){
                toProcessList.add(mutation);
                if(toProcessList.size()==writeBatchSize){
                    if(toProcess==null)
                        toProcess = new Pair[writeBatchSize];
                    toProcess = toProcessList.toArray(toProcess);
                    doWrite(ctx,toProcess);
                    toProcessList.clear();
                }
            }
            if(toProcessList.size()>0){
                toProcess = toProcessList.toArray(new Pair[toProcessList.size()]);
                doWrite(ctx,toProcess);
            }
        }catch(IOException ioe){
            /*
             * We are hinging on an undocumented implementation of how HRegion.put(Pair<Put,Integer>[]) works.
             *
             * HRegion.put(Pair<Put,Integer>[]) will throw an IOException
             * if the WALEdit doesn't succeed, but only a single WALEdit write occurs,
             * containing all the individual edits for the Pair[]. As a result, if we get an IOException,
             * it's because we were unable to write ANY records to the WAL, so we can safely assume that
             * all the puts failed and can be safely retried.
             */
            failed=true;
            MutationResult result = new MutationResult(MutationResult.Code.FAILED,ioe.getClass().getSimpleName()+":"+ioe.getMessage());
            for (Pair<Mutation,Integer> pair : mutations) {
                ctx.result(pair.getFirst(),result);
            }
        }
    }

    private void doWrite(WriteContext ctx, Pair<Mutation, Integer>[] toProcess) throws IOException {
        if (SIObserver.doesTableNeedSI(region)) {
            doSIWrite(ctx, toProcess);
        } else {
            mutateAndPostProcess(ctx, toProcess, null, toProcess, null, false);
        }
    }

    private void doSIWrite(WriteContext ctx, Pair<Mutation, Integer>[] toProcess) throws IOException {
        final Transactor<IHTable, Put, Get, Scan, Mutation, Result, KeyValue, byte[], ByteBuffer, HRowLock> transactor = HTransactorFactory.getTransactor();
        final Pair<Mutation, Integer>[] mutationsAndLocks = new Pair[toProcess.length];
        final Set<Long>[] conflictingChildren = new Set[toProcess.length];

        Map<ByteBuffer, HRowLock> locks = new HashMap<ByteBuffer, HRowLock>();
        try {
            for (int i = 0; i < toProcess.length; i++) {
                final PutToRun<Mutation, HRowLock> putToRun = transactor.preProcessBatchPut(new HbRegion(region), null,
                        (Put) toProcess[i].getFirst(), locks);
                mutationsAndLocks[i] = putToRun.putAndLock;
                conflictingChildren[i] = putToRun.conflictingChildren;
            }

            mutateAndPostProcess(ctx, toProcess, transactor, mutationsAndLocks, conflictingChildren, true);
        } finally {
            cleanupLockedRows(transactor, locks);
        }
    }

    private void cleanupLockedRows(Transactor<IHTable, ?, ?, ?, ?, ?, ?, ?, ?, HRowLock> transactor, Map<ByteBuffer, HRowLock> locks) throws IOException {
        IHTable table = new HbRegion(region);
        for (HRowLock lock : locks.values()) {
            if (lock == null) {
                continue;
            }
            try {
                transactor.cleanupLock(table, lock);
            } catch (Throwable t) {
                LOG.error("Exception while cleaning up locks", t);
                // ignore
            }
        }
    }

    private void mutateAndPostProcess(WriteContext ctx, Pair<Mutation, Integer>[] originalToProcess,
                                      Transactor<IHTable, Put, Get, Scan, Mutation, Result, KeyValue, byte[], ByteBuffer, HRowLock> transactor,
                                      Pair<Mutation, Integer>[] newToProcess, Set<Long>[] conflictingChildren, boolean siNeeded) throws IOException {
        final OperationStatus[] status = region.batchMutate(newToProcess);

        for (int i = 0; i < status.length; i++) {
            OperationStatus stat = status[i];
            Mutation mutation = originalToProcess[i].getFirst();
            switch (stat.getOperationStatusCode()) {
                case NOT_RUN:
                    ctx.notRun(mutation);
                    break;
                case BAD_FAMILY:
                case FAILURE:
                    ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, stat.getExceptionMsg()));
                default:
                    try {
                        if (siNeeded) {
                            transactor.postProcessBatchPut(new HbRegion(region), (Put) originalToProcess[i].getFirst(),
                                    new HRowLock(newToProcess[i].getSecond()), conflictingChildren[i]);
                        }
                        ctx.success(mutation);
                    } catch (Throwable t) {
                        ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, t.getMessage()));
                    }
                    break;
            }
        }
    }
}
