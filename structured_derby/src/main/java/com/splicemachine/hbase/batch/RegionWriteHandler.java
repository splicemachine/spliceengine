package com.splicemachine.hbase.batch;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.hbase.writer.MutationResult;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.coprocessors.RollForwardQueueMap;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.tools.ResettableCountDownLatch;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class RegionWriteHandler implements WriteHandler {
    static final Logger LOG = Logger.getLogger(RegionWriteHandler.class);

    private final HRegion region;
    private final List<Mutation> mutations = Lists.newArrayList();
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
        mutation.setAttribute(IndexSet.INDEX_UPDATED, IndexSet.INDEX_ALREADY_UPDATED);
        if (HRegion.rowIsInRange(ctx.getRegion().getRegionInfo(), mutation.getRow())) {
            mutations.add(mutation);
        } else {
            ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, "WrongRegion"));
        }
    }

    @SuppressWarnings({"unchecked", "unused"})
    @Override
    public void finishWrites(final WriteContext ctx) throws IOException {

        //make sure that the write aborts if the caller disconnects
        RpcCallContext currentCall = HBaseServer.getCurrentCall();
        if(currentCall!=null)
            currentCall.throwExceptionIfCallerDisconnected();

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
        try {
            Collection<Mutation> filteredMutations = Collections2.filter(mutations, new Predicate<Mutation>() {
                @Override
                public boolean apply(@Nullable Mutation input) {
                    return ctx.canRun(input);
                }
            });
//            if(ctx.getRegion().isClosed()||ctx.getRegion().isClosing()){
//                for(Mutation mutation:filteredMutations){
//                    ctx.failed(mutation,new MutationResult(MutationResult.Code.FAILED,"NotServingRegion"));
//                }
//            }
            Mutation[] toProcess = new Mutation[filteredMutations.size()];
            filteredMutations.toArray(toProcess);

            doWrite(ctx,toProcess);
        } catch (WriteConflict wce) {
            MutationResult result = new MutationResult(MutationResult.Code.WRITE_CONFLICT, wce.getClass().getSimpleName() + ":" + wce.getMessage());
            for (Mutation mutation : mutations) {
                ctx.result(mutation, result);
            }
        } catch (IOException ioe) {
            /*
             * We are hinging on an undocumented implementation of how HRegion.put(Pair<Put,Integer>[]) works.
             *
             * HRegion.put(Pair<Put,Integer>[]) will throw an IOException
             * if the WALEdit doesn't succeed, but only a single WALEdit write occurs,
             * containing all the individual edits for the Pair[]. As a result, if we get an IOException,
             * it's because we were unable to write ANY records to the WAL, so we can safely assume that
             * all the puts failed and can be safely retried.
             */
            MutationResult result = new MutationResult(MutationResult.Code.FAILED, ioe.getClass().getSimpleName() + ":" + ioe.getMessage());
            for (Mutation mutation : mutations) {
                ctx.result(mutation, result);
            }
        }
    }

    private void doWrite(WriteContext ctx, Mutation[] toProcess) throws IOException {
        final OperationStatus[] status = SIObserver.doesTableNeedSI(region) ? doSIWrite(toProcess) : doNonSIWrite(toProcess);
        for (int i = 0; i < status.length; i++) {
            OperationStatus stat = status[i];
            Mutation mutation = toProcess[i];
            switch (stat.getOperationStatusCode()) {
                case NOT_RUN:
                    ctx.notRun(mutation);
                    break;
                case BAD_FAMILY:
                case FAILURE:
                    ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, stat.getExceptionMsg()));
                    break;
                default:
                    ctx.success(mutation);
                    break;
            }
        }
    }

    private OperationStatus[] doNonSIWrite(Mutation[] toProcess) throws IOException {
        Pair<Mutation, Integer>[] pairsToProcess = new Pair[toProcess.length];
        for (int i=0; i<toProcess.length; i++) {
            pairsToProcess[i] = new Pair<Mutation, Integer>(toProcess[i], null);
        }
        return region.batchMutate(pairsToProcess);
    }

    private OperationStatus[] doSIWrite(Mutation[] toProcess) throws IOException {
        final Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = HTransactorFactory.getTransactor();
        final String tableName = region.getTableDesc().getNameAsString();
        final RollForwardQueue<byte[],ByteBuffer> rollForwardQueue = RollForwardQueueMap.lookupRollForwardQueue(tableName);
        return transactor.processPutBatch(new HbRegion(region), rollForwardQueue, toProcess);
    }

}
