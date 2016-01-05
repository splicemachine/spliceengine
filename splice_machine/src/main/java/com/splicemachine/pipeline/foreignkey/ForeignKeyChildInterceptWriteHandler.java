package com.splicemachine.pipeline.foreignkey;

import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;

import java.io.IOException;

/**
 * Intercepts insert/updates to a FK constraint backing index and sends the rowKey over to the referenced primary-key or
 * unique-index region for existence checking.
 */
public class ForeignKeyChildInterceptWriteHandler implements WriteHandler{

    private final WriteCoordinator writeCoordinator;
    private final long referencedConglomerateNumber;
    private final ForeignKeyViolationProcessor violationProcessor;

    private RecordingCallBuffer<KVPair> referencedTableCallBuffer;
    private Partition table;

    public ForeignKeyChildInterceptWriteHandler(long referencedConglomerateNumber,
                                                FKConstraintInfo fkConstraintInfo,
                                                PipelineExceptionFactory exceptionFactory) {
        this.referencedConglomerateNumber = referencedConglomerateNumber;
        this.writeCoordinator = PipelineDriver.driver().writeCoordinator();
        this.violationProcessor = new ForeignKeyViolationProcessor(
                new ForeignKeyViolationProcessor.ChildFkConstraintContextProvider(fkConstraintInfo),
                exceptionFactory);
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (isForeignKeyInterceptNecessary(mutation.getType())) {
            KVPair kvPair = new KVPair(mutation.getRowKey(), SIConstants.EMPTY_BYTE_ARRAY, KVPair.Type.FOREIGN_KEY_PARENT_EXISTENCE_CHECK);
            try {
                initTargetCallBuffer(ctx);
                referencedTableCallBuffer.add(kvPair);
                ctx.success(mutation);
            } catch (Exception e) {
                violationProcessor.failWrite(e, ctx);
            }
        }
        ctx.sendUpstream(mutation);
    }

    /* This WriteHandler doesn't do anything when, for example, we delete from the FK backing index. */
    private boolean isForeignKeyInterceptNecessary(KVPair.Type type) {
        return type == KVPair.Type.INSERT || type == KVPair.Type.UPDATE || type == KVPair.Type.UPSERT;
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        try {
            referencedTableCallBuffer.flushBuffer();
        } catch (Exception e) {
            violationProcessor.failWrite(e, ctx);
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        try {
            if(referencedTableCallBuffer!=null){
                    referencedTableCallBuffer.close();
            }
        } catch (Exception e) {
            violationProcessor.failWrite(e, ctx);
        }finally{
            if(table!=null)
                table.close();
        }
    }

    /* Only need to create the CallBuffer once, but not until we have a WriteContext */
    private void initTargetCallBuffer(WriteContext ctx) throws IOException{
        if (referencedTableCallBuffer == null) {
            table = SIDriver.driver().getTableFactory().getTable(Long.toString((referencedConglomerateNumber)));
            referencedTableCallBuffer = writeCoordinator.writeBuffer(table,ctx.getTxn());
        }
    }

    @Override
    public String toString() {
        return "ForeignKeyChildInterceptWriteHandler{parentTable='" + referencedConglomerateNumber + '\'' + '}';
    }


}
