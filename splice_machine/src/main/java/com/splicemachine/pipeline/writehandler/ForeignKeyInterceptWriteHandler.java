package com.splicemachine.pipeline.writehandler;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.constraint.ConstraintViolation;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.pipeline.impl.WriteFailedException;
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
        KVPair kvPair = new KVPair(mutation.getRow(), HConstants.EMPTY_BYTE_ARRAY, KVPair.Type.FOREIGN_KEY_CHECK);
        try {
            initTargetCallBuffer(ctx);
            referencedTableCallBuffer.add(kvPair);
            ctx.sendUpstream(mutation);
            ctx.success(mutation);
        } catch (Exception e) {
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }
    }

    @Override
    public void next(List<KVPair> mutations, WriteContext ctx) {
        throw new UnsupportedOperationException("never called");
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        try {
            referencedTableCallBuffer.flushBuffer();
        } catch (WriteFailedException wfe) {
            ctx.failed(null, WriteResult.failed(wfe.getMessage()));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        try {
            referencedTableCallBuffer.close();
        } catch (WriteFailedException wfe) {
            ctx.failed(null, WriteResult.failed(wfe.getMessage()));
        } catch (ExecutionException e) {
            addInfoToException(e);
        } catch (Exception e) {
            throw new IOException(e);
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

    private void addInfoToException(ExecutionException originalException) throws ConstraintViolation.ForeignKeyConstraintViolation {
        Throwable t = originalException;
        while ((t = t.getCause()) != null) {
            if (t instanceof RetriesExhaustedWithDetailsException) {
                RetriesExhaustedWithDetailsException retriesException = (RetriesExhaustedWithDetailsException) t;
                if (retriesException.getCauses() != null && !retriesException.getCauses().isEmpty()) {
                    Throwable cause = retriesException.getCause(0);
                    if (cause instanceof ConstraintViolation.ForeignKeyConstraintViolation) {
                        throw new ConstraintViolation.ForeignKeyConstraintViolation(constraintContext);
                    }
                }
            }
        }
    }
}
