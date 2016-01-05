package com.splicemachine.pipeline.foreignkey;

import com.google.common.collect.Maps;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Intercepts deletes from a parent table (primary key or unique index) and sends the rowKey over to the referencing
 * indexes to check for its existence.
 */
@NotThreadSafe
public class ForeignKeyParentInterceptWriteHandler implements WriteHandler{

    private final WriteCoordinator writeCoordinator;
    private final List<Long> referencingIndexConglomerateIds;
    private final Map<Long, RecordingCallBuffer<KVPair>> callBufferMap = Maps.newHashMap();
    private final ForeignKeyViolationProcessor violationProcessor;
    private Partition destPartition;

    public ForeignKeyParentInterceptWriteHandler(String parentTableName,
                                                 List<Long> referencingIndexConglomerateIds,
                                                 PipelineExceptionFactory exceptionFactory) {
        this.referencingIndexConglomerateIds = referencingIndexConglomerateIds;
        this.writeCoordinator = PipelineDriver.driver().writeCoordinator();
        this.violationProcessor = new ForeignKeyViolationProcessor(
                new ForeignKeyViolationProcessor.ParentFkConstraintContextProvider(parentTableName),exceptionFactory);
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (isForeignKeyInterceptNecessary(mutation.getType())) {
            KVPair kvPair = new KVPair(mutation.getRowKey(), SIConstants.EMPTY_BYTE_ARRAY, KVPair.Type.FOREIGN_KEY_CHILDREN_EXISTENCE_CHECK);
            try {
                for (long indexConglomerateId : this.referencingIndexConglomerateIds) {
                    getTargetCallBuffer(ctx, indexConglomerateId).add(kvPair);
                }
                ctx.success(mutation);
            } catch (Exception e) {
                violationProcessor.failWrite(e, ctx);
            }
        }
        ctx.sendUpstream(mutation);
    }

    private boolean isForeignKeyInterceptNecessary(KVPair.Type type) {
        /* We exist to prevent updates/deletes of rows from the parent table which are referenced by a child.
         * Since we are a WriteHandler on a primary-key or unique-index we can just handle deletes. */
        return type == KVPair.Type.DELETE;
    }


    @Override
    public void flush(WriteContext ctx) throws IOException {
        try {
            for (CallBuffer<KVPair> callBuffer : callBufferMap.values()) {
                callBuffer.flushBuffer();
            }
        } catch (Exception e) {
            violationProcessor.failWrite(e, ctx);
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        try {
            for (CallBuffer<KVPair> callBuffer : callBufferMap.values()) {
                callBuffer.close();
            }
        } catch (Exception e) {
            violationProcessor.failWrite(e, ctx);
        }finally{
            destPartition.close();
        }
    }

    /* Only need to create the CallBuffer once, but not until we have a WriteContext */
    private RecordingCallBuffer<KVPair> getTargetCallBuffer(WriteContext ctx, long conglomerateId) throws IOException{
        if (!callBufferMap.containsKey(conglomerateId)) {
            if(destPartition==null){
                PartitionFactory pf=SIDriver.driver().getTableFactory();
                destPartition=pf.getTable(String.valueOf(conglomerateId));
            }
            RecordingCallBuffer<KVPair> callBuffer = writeCoordinator.writeBuffer(destPartition, ctx.getTxn());
            callBufferMap.put(conglomerateId, callBuffer);
        }
        return callBufferMap.get(conglomerateId);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
