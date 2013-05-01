package com.splicemachine.hbase.batch;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.tools.ResettableCountDownLatch;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public class RegionWriteHandler implements WriteHandler {
    private final HRegion region;
    private final List<Put> puts = Lists.newArrayList();
    private final List<Delete> deletes = Lists.newArrayListWithExpectedSize(0);

    private final ResettableCountDownLatch writeLatch;

    public RegionWriteHandler(HRegion region,ResettableCountDownLatch writeLatch) {
        this.region = region;
        this.writeLatch = writeLatch;
    }

    @Override
    public void next(Mutation mutation, WriteContext ctx) {
        /*
         * Write-wise, we are at the end of the line, so make sure that we don't run through
         * another write-pipeline when the Region actually does it's writing
         */
        mutation.setAttribute(IndexSet.INDEX_UPDATED,IndexSet.INDEX_ALREADY_UPDATED);
        if(mutation instanceof Put)
            puts.add((Put)mutation);
        else
            deletes.add((Delete)mutation);
    }

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
        try{
            Collection<Put> putsToWrite = Collections2.filter(puts,new Predicate<Put>() {
                @Override
                public boolean apply(@Nullable Put input) {
                    return ctx.canRun(input);
                }
            });
            @SuppressWarnings("unchecked") Pair<Put,Integer>[] putsAndLocks = new Pair[putsToWrite.size()];

            int pos=0;
            for(Put put:putsToWrite){
                putsAndLocks[pos] = Pair.newPair(put,null);
                pos++;
            }
            OperationStatus[] status = region.put(putsAndLocks);

            for(int i=0;i<status.length;i++){
                OperationStatus stat = status[i];
                Put put = putsAndLocks[i].getFirst();
                switch (stat.getOperationStatusCode()) {
                    case NOT_RUN:
                        ctx.notRun(put);
                        break;
                    case BAD_FAMILY:
                    case FAILURE:
                        ctx.failed(put, stat.getExceptionMsg());
                        failed=true;
                    default:
                        ctx.success(put);
                        break;
                }
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
            for(Put put:puts){
                ctx.result(put,result);
            }
        }
        if(failed){
            //no need to run deletes, the puts failed
            for(Delete delete:deletes)
                ctx.notRun(delete);
        }else{
            for(Delete delete:deletes){
                if(failed)
                    ctx.notRun(delete);
                else if(ctx.canRun(delete)){
                    try{
                        region.delete(delete,null,true);
                    }catch(IOException ioe){
                        failed=true;
                        ctx.failed(delete, ioe.getClass().getSimpleName()+":"+ioe.getMessage());
                    }
                }
            }
        }
    }
}
