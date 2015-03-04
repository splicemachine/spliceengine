package com.splicemachine.pipeline.writehandler.foreignkey;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.db.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Intercepts insert/updates to a FK constraint backing index and sends the rowKey over to the referenced primary-key or
 * unique-index region for existence checking.
 */
public class ForeignKeyChildInterceptWriteHandler implements WriteHandler {

    private final WriteCoordinator writeCoordinator;
    private final byte[] parentTableName;
    private final String parentTableNameString;
    private final ForeignKeyViolationProcessor violationProcessor;

    private RecordingCallBuffer<KVPair> referencedTableCallBuffer;

    public ForeignKeyChildInterceptWriteHandler(byte[] parentTableName, ForeignKeyConstraintDescriptor constraintDescriptor) {
        this.parentTableName = parentTableName;
        this.parentTableNameString = Bytes.toString(parentTableName);
        this.writeCoordinator = SpliceDriver.driver().getTableWriter();
        this.violationProcessor = new ForeignKeyViolationProcessor(new ForeignKeyViolationProcessor.ChildFkConstraintContextProvider(constraintDescriptor));
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (isForeignKeyInterceptNecessary(mutation.getType())) {
            KVPair kvPair = new KVPair(mutation.getRowKey(), HConstants.EMPTY_BYTE_ARRAY, KVPair.Type.FOREIGN_KEY_PARENT_EXISTENCE_CHECK);
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
    public void next(List<KVPair> mutations, WriteContext ctx) {
        throw new UnsupportedOperationException("never called");
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
            referencedTableCallBuffer.close();
        } catch (Exception e) {
            violationProcessor.failWrite(e, ctx);
        }
    }

    /* Only need to create the CallBuffer once, but not until we have a WriteContext */
    private void initTargetCallBuffer(WriteContext ctx) {
        if (referencedTableCallBuffer == null) {
            referencedTableCallBuffer = writeCoordinator.writeBuffer(parentTableName, ctx.getTxn());
        }
    }

    @Override
    public String toString() {
        return "ForeignKeyInterceptWriteHandler{parentTable='" + parentTableNameString + '\'' + '}';
    }


}
