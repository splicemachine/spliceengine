/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.pipeline.writehandler;

import com.splicemachine.access.api.NotServingPartitionException;
import com.splicemachine.access.api.RegionBusyException;
import com.splicemachine.access.api.WrongPartitionException;
import com.splicemachine.concurrent.ResettableCountDownLatch;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.constraint.BatchConstraintChecker;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.WriteConflict;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Predicate;
import splice.com.google.common.collect.Collections2;
import splice.com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class PartitionWriteHandler implements WriteHandler {
    static final Logger LOG = Logger.getLogger(PartitionWriteHandler.class);
    protected final TransactionalRegion region;
    protected List<KVPair> mutations = Lists.newArrayList();
    protected ResettableCountDownLatch writeLatch;
    protected BatchConstraintChecker constraintChecker;

    public PartitionWriteHandler(TransactionalRegion region,
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
            if (kvPair.getType() == KVPair.Type.CANCEL){
                mutations.add(new KVPair(kvPair.getRowKey(), kvPair.getValue(), KVPair.Type.DELETE));
            }
            else
                mutations.add(kvPair);
            ctx.sendUpstream(kvPair);
        }
    }

    @Override
    public void flush(final WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "flush");
        //make sure that the write aborts if the caller disconnects
        ctx.getCoprocessorEnvironment().ensureNetworkOpen();
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
                return ctx.canRun(input);
            }
        });
        try {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Flush Writing rows=%d, table=%s", filteredMutations.size(), region.getTableName());
            doWrite(ctx, filteredMutations);
        } catch (IOException wce) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("flush, ", wce);
            }
            Throwable t = ctx.exceptionFactory().processPipelineException(wce);
            if(t instanceof WriteConflict){
                WriteResult result=new WriteResult(Code.WRITE_CONFLICT,wce.getMessage());
                for(KVPair mutation : filteredMutations){
                    ctx.result(mutation,result);
                }
            }else if(t instanceof NotServingPartitionException){
                WriteResult result = WriteResult.notServingRegion();
                for (KVPair mutation : filteredMutations) {
                    ctx.result(mutation, result);
                }
            }else if(t instanceof RegionBusyException){
                WriteResult result = WriteResult.regionTooBusy();
                for (KVPair mutation : filteredMutations) {
                    ctx.result(mutation, result);
                }
            }else if(t instanceof WrongPartitionException){
                //this shouldn't happen, but just in case
                WriteResult result = WriteResult.wrongRegion();
                for (KVPair mutation : filteredMutations) {
                    ctx.result(mutation, result);
                }
            } else{
                /*
                 * Paritition.put(Put[]) will throw an IOException
                 * if the WALEdit doesn't succeed, but only a single WALEdit write occurs,
                 * containing all the individual edits for the Pair[]. As a result, if we get an IOException,
                 * it's because we were unable to write ANY records to the WAL, so we can safely assume that
                 * all the puts failed and can be safely retried.
                 */
                LOG.error("Unexpected exception", wce);
                WriteResult result=WriteResult.failed(wce.getClass().getSimpleName()+":"+wce.getMessage());
                for(KVPair mutation : filteredMutations){
                    ctx.result(mutation,result);
                }
            }
        } finally {
            filteredMutations.clear();
        }
    }

    private void doWrite(WriteContext ctx, Collection<KVPair> toProcess) throws IOException {
        assert toProcess!=null; //won't ever happen, but it's a nice safety check
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "doWrite {region=%s, records=%d}", ctx.getRegion().getName(),toProcess.size());

        Iterable<MutationStatus> status = region.bulkWrite(
                ctx.getTxn(),
                SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.PACKED_COLUMN_BYTES,
                constraintChecker,
                toProcess,
                ctx.skipConflictDetection(),
                ctx.skipWAL(),
                ctx.rollforward()
        );

        int i = 0;
        int failed = 0;
        Iterator<MutationStatus> statusIter = status.iterator();
        Iterator<KVPair> mutationIter = toProcess.iterator();
        int count = 0;
        while(statusIter.hasNext()){
            count++;
            if(!mutationIter.hasNext())
                throw new IllegalStateException("MutationStatus result is not the same size as the mutations collection!");
            MutationStatus stat = statusIter.next();
            KVPair mutation = mutationIter.next();
            if(stat.isNotRun())
                ctx.notRun(mutation);
            else if(stat.isSuccess()){
                ctx.success(mutation);
            } else{
                //assume it's a failure
                //see if it's due to constraints, otherwise just pass it through
                if (constraintChecker != null && constraintChecker.matches(stat)) {
                    ctx.result(mutation, constraintChecker.asWriteResult(stat));
                }else if (stat.errorMessage().contains("Write conflict")) {
                    WriteResult conflict = new WriteResult(Code.WRITE_CONFLICT,stat.errorMessage());
                    ctx.result(mutation, conflict);
                }else {
                    ctx.failed(mutation, WriteResult.failed(stat.errorMessage()));
                }
                failed++;
            }
        }

        region.updateWriteRequests(count - failed);
    }


    @Override
    public void close(WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "close");
        mutations.clear();
        mutations = null; // Dereference
    }

}
