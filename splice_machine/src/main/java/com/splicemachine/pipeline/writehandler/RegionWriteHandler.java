package com.splicemachine.pipeline.writehandler;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.concurrent.ResettableCountDownLatch;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.constraint.BatchConstraintChecker;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.utils.SpliceLogUtils;
import com.yammer.metrics.core.*;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class RegionWriteHandler implements WriteHandler {
    protected static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    static final Logger LOG = Logger.getLogger(RegionWriteHandler.class);
    private final TransactionalRegion region;
    private List<KVPair> mutations = Lists.newArrayList();
    private ResettableCountDownLatch writeLatch;
    private BatchConstraintChecker constraintChecker;

    public RegionWriteHandler(TransactionalRegion region,
                              ResettableCountDownLatch writeLatch,
                              BatchConstraintChecker constraintChecker) {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "regionWriteHandler create");
        this.region = region;
        this.writeLatch = writeLatch;
        this.constraintChecker = constraintChecker;
        this.mutations = Lists.newArrayList();
    }

    @Override
    public void next(KVPair kvPair, WriteContext ctx) {
        /*
         * Write-wise, we are at the end of the line, so make sure that we don't run through
         * another write-pipeline when the Region actually does it's writing
         */
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "next kvPair=%s, ctx=%s",kvPair,ctx);
        if(region.isClosed())
            ctx.failed(kvPair,WriteResult.notServingRegion());
        else if(!region.rowInRange(kvPair.rowKeySlice()))
            ctx.failed(kvPair, WriteResult.wrongRegion());
        else {
            mutations.add(kvPair);
            ctx.sendUpstream(kvPair);
        }
    }

    @Override
    public void flush(final WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "flush");
        //make sure that the write aborts if the caller disconnects
        derbyFactory.checkCallerDisconnect(ctx.getRegion());
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
        Collection<KVPair> filteredMutations = Collections2.filter(mutations, new Predicate<KVPair>() {
            @Override
            public boolean apply(KVPair input) {
                return ctx.canRun(input) && input.getType() != KVPair.Type.FOREIGN_KEY_CHECK;
            }
        });
        try {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Flush Writing rows=%d, table=%s", filteredMutations.size(), region.getTableName());
            doWrite(ctx, filteredMutations);
        } catch (WriteConflict wce) {
            WriteResult result = new WriteResult(Code.WRITE_CONFLICT, wce.getMessage());
            for (KVPair mutation : filteredMutations) {
                ctx.result(mutation, result);
            }
        } catch (NotServingRegionException nsre) {
            WriteResult result = WriteResult.notServingRegion();
            for (KVPair mutation : filteredMutations) {
                ctx.result(mutation, result);
            }
        } catch (RegionTooBusyException rtbe) {
            WriteResult result = WriteResult.regionTooBusy();
            for (KVPair mutation : filteredMutations) {
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
            WriteResult result = WriteResult.failed(ioe.getClass().getSimpleName() + ":" + ioe.getMessage());
            for (KVPair mutation : filteredMutations) {
                ctx.result(mutation, result);
            }
        } finally {
            filteredMutations.clear();
        }
    }

    private void doWrite(WriteContext ctx, Collection<KVPair> toProcess) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "doWrite {region=%s, records=%d}", ctx.getRegion().getRegionNameAsString(), toProcess != null ? toProcess.size() : 0);

        OperationStatus[] status = region.bulkWrite(
                ctx.getTxn(),
                SpliceConstants.DEFAULT_FAMILY_BYTES,
                SpliceConstants.PACKED_COLUMN_BYTES,
                constraintChecker,
                toProcess
        );

        int i = 0;
        int failed = 0;
        for (KVPair mutation : toProcess) {
            OperationStatus stat = status[i];
            switch (stat.getOperationStatusCode()) {
                case NOT_RUN:
                    ctx.notRun(mutation);
                    break;
                case SUCCESS:
                    ctx.success(mutation);
                    break;
                case FAILURE:
                    //see if it's due to constraints, otherwise just pass it through
                    if (constraintChecker != null && constraintChecker.matches(stat)) {
                        ctx.result(mutation, constraintChecker.asWriteResult(stat));
                        break;
                    }
                default:
                    failed++;
                    ctx.failed(mutation, WriteResult.failed(stat.getExceptionMsg()));
                    break;
            }
            i++;
        }
        region.updateWriteRequests(toProcess.size() - failed);
    }


    @Override
    public void next(List<KVPair> mutations, WriteContext ctx) {
        throw new RuntimeException("Not Supported");
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "close");
        mutations.clear();
        mutations = null; // Dereference
    }

}
