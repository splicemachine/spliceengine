package com.splicemachine.pipeline.writehandler;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.constraint.ConstraintViolation;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.pipeline.impl.WriteResult;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Intercepts writes to a FK constraint backing index and sends the rowKey over to the referenced primary-key or
 * unique-index region for existence checking.
 */
public class ForeignKeyInterceptWriteHandler implements WriteHandler {

    private final WriteCoordinator writeCoordinator;
    private final byte[] parentTableName;
    private final String parentTableNameString;
    private final ConstraintContext constraintContext;

    private RecordingCallBuffer<KVPair> referencedTableCallBuffer;

    public ForeignKeyInterceptWriteHandler(byte[] parentTableName, ConstraintContext constraintContext) {
        this.parentTableName = parentTableName;
        this.constraintContext = constraintContext;
        this.parentTableNameString = Bytes.toString(parentTableName);
        this.writeCoordinator = SpliceDriver.driver().getTableWriter();
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (isForeignKeyInterceptNecessary(mutation.getType())) {
            KVPair kvPair = new KVPair(mutation.getRowKey(), HConstants.EMPTY_BYTE_ARRAY, KVPair.Type.FOREIGN_KEY_CHECK);
            try {
                initTargetCallBuffer(ctx);
                referencedTableCallBuffer.add(kvPair);
                ctx.success(mutation);
            } catch (Exception e) {
                failWrite(e, ctx);
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
            failWrite(e, ctx);
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        try {
            referencedTableCallBuffer.close();
        } catch (Exception e) {
            failWrite(e, ctx);
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

    /**
     * We apply the details of the foreign key violation exception message here in the InterceptWriteHandler because
     * there is one instance of this class per FK, and only this class has all of the info necessary for exception
     * message creation (on the other hand there is only one CheckWriteHandler per table, it doesn't know which FK
     * actually fails the check). This code looks fragile but it is validated by every single FK IT test method.
     * Breakages in this method would result in all FK ITs failing.
     */
    private void failWrite(Exception originalException, WriteContext ctx) {
        Throwable t = originalException;
        while ((t = t.getCause()) != null) {
            if (t instanceof RetriesExhaustedWithDetailsException) {
                RetriesExhaustedWithDetailsException retriesException = (RetriesExhaustedWithDetailsException) t;
                if (retriesException.getCauses() != null && !retriesException.getCauses().isEmpty()) {
                    Throwable cause = retriesException.getCause(0);
                    if (cause instanceof ConstraintViolation.ForeignKeyConstraintViolation) {
                        ConstraintViolation.ForeignKeyConstraintViolation v = (ConstraintViolation.ForeignKeyConstraintViolation) cause;
                        byte[] failedRowKey = BytesUtil.fromHex(v.getConstraintContext().getMessages()[0]);
                        ctx.result(failedRowKey, new WriteResult(Code.FOREIGN_KEY_VIOLATION, constraintContext));
                    }
                }
            }
        }
    }
}
