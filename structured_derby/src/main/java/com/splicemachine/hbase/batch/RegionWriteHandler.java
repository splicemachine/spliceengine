package com.splicemachine.hbase.batch;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.HBaseServerUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.coprocessors.RollForwardQueueMap;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.tools.ResettableCountDownLatch;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class RegionWriteHandler implements WriteHandler {
    static final Logger LOG = Logger.getLogger(RegionWriteHandler.class);

    private final HRegion region;
    private final List<KVPair> mutations = Lists.newArrayList();
    private final ResettableCountDownLatch writeLatch;
    private final int writeBatchSize;
    private RollForwardQueue queue;
	private BatchConstraintChecker constraintChecker;

    public RegionWriteHandler(HRegion region, ResettableCountDownLatch writeLatch, int writeBatchSize,
															BatchConstraintChecker constraintChecker) {
				this(region,writeLatch,writeBatchSize,null,constraintChecker);
    }

    public RegionWriteHandler(HRegion region,
                              ResettableCountDownLatch writeLatch,
                              int writeBatchSize,
                              RollForwardQueue queue,
															BatchConstraintChecker constraintChecker){
        this.region = region;
        this.writeLatch = writeLatch;
        this.writeBatchSize = writeBatchSize;
        this.queue = queue;
				this.constraintChecker = constraintChecker;
    }

    @Override
    public void next(KVPair kvPair, WriteContext ctx) {
        /*
         * Write-wise, we are at the end of the line, so make sure that we don't run through
         * another write-pipeline when the Region actually does it's writing
         */
        if(region.isClosing()||region.isClosed())
            ctx.failed(kvPair,WriteResult.notServingRegion());
        else if (!HRegion.rowIsInRange(region.getRegionInfo(), kvPair.getRow())) {
            ctx.failed(kvPair, WriteResult.wrongRegion());
        } else {
            mutations.add(kvPair);
            ctx.sendUpstream(kvPair);
        }
    }

    private Mutation getMutation(KVPair kvPair, WriteContext ctx, boolean si) throws IOException {
        byte[] rowKey = kvPair.getRow();
        byte[] value = kvPair.getValue();
        Mutation mutation;
        Put put;
        switch (kvPair.getType()) {
            case UPDATE:
            	if (si)
            		put = SpliceUtils.createPut(rowKey,ctx.getTransactionId());
            	else
            		throw new RuntimeException("Updating a non si table?");
                put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES,value);
                mutation = put;
                mutation.setAttribute(Puts.PUT_TYPE,Puts.FOR_UPDATE);
                break;
            case DELETE:
            	if (si)
            		mutation = SpliceUtils.createDeletePut(ctx.getTransactionId(),rowKey); // Probably need this as well...
            	else
            		throw new RuntimeException("Deleting a non si table?");
                break;
            default:
            	if (si)
            		put = SpliceUtils.createPut(rowKey,ctx.getTransactionId());
            	else 
            		put = new Put(rowKey);
                put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES,ctx.getTransactionTimestamp(),value);
                mutation = put;
        }
        mutation.setAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        return mutation;

    }

    @Override
    public void finishWrites(final WriteContext ctx) throws IOException {
        //make sure that the write aborts if the caller disconnects
				HBaseServerUtils.checkCallerDisconnect(ctx.getCoprocessorEnvironment().getRegion(), "RegionWrite");

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
            public boolean apply(@Nullable KVPair input) {
                return ctx.canRun(input);
            }
        });
        try {

            if(LOG.isTraceEnabled())
                LOG.trace("Writing "+ filteredMutations.size()+" rows to table " + region.getTableDesc().getNameAsString());
            doWrite(ctx,filteredMutations);
        } catch (WriteConflict wce) {
            WriteResult result = new WriteResult(WriteResult.Code.WRITE_CONFLICT, wce.getMessage());
            for (KVPair mutation : filteredMutations) {
                ctx.result(mutation, result);
            }
        }catch(NotServingRegionException nsre){
            WriteResult result = WriteResult.notServingRegion();
            for (KVPair mutation : filteredMutations) {
                ctx.result(mutation, result);
            }
        }catch(RegionTooBusyException rtbe){
            WriteResult result = WriteResult.regionTooBusy();
            for(KVPair mutation:filteredMutations){
                ctx.result(mutation,result);
            }
        }catch (IOException ioe) {
            LOG.error(ioe);
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
        }
    }

    private void doWrite(WriteContext ctx, Collection<KVPair> toProcess) throws IOException {
        boolean siTable = SIObserver.doesTableNeedSI(region);
        final OperationStatus[] status = siTable ? doSIWrite(toProcess,ctx) : doNonSIWrite(toProcess,ctx);
        int i=0;
        int failed=0;
        for(KVPair mutation:toProcess){
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
										if(constraintChecker!=null && constraintChecker.matches(stat)){
												ctx.result(mutation,constraintChecker.asWriteResult(stat));
												break;
										}
								default:
                    failed++;
                    ctx.failed(mutation,WriteResult.failed(stat.getExceptionMsg()));
                    break;
            }
            i++;
        }
        HRegionUtil.updateWriteRequests(region, toProcess.size()-failed); 
    }

    private OperationStatus[] doNonSIWrite(Collection<KVPair> toProcess,WriteContext ctx) throws IOException {
        Mutation[] pairsToProcess = new Mutation[toProcess.size()];
        int i=0;
        for(KVPair pair:toProcess){
            pairsToProcess[i] = getMutation(pair,ctx,false);
            i++;
        }
        return region.batchMutate(pairsToProcess);
    }

    private OperationStatus[] doSIWrite(Collection<KVPair> toProcess,WriteContext ctx) throws IOException {
        final Transactor<IHTable, Mutation,Put> transactor = HTransactorFactory.getTransactor();
        final String tableName = region.getTableDesc().getNameAsString();
        if(queue==null)
            queue =  RollForwardQueueMap.lookupRollForwardQueue(tableName);
				return transactor.processKvBatch(new HbRegion(region),queue,
								SpliceConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,
								toProcess,ctx.getTransactionId(),constraintChecker);
    }

	@Override
	public void next(List<KVPair> mutations, WriteContext ctx) {
		// XXX JLEACH TODO
		throw new RuntimeException("Not Supported");
	}

}
