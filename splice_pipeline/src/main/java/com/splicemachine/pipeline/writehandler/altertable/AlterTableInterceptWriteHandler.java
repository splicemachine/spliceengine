package com.splicemachine.pipeline.writehandler.altertable;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Jeff Cunningham
 *         Date: 3/16/15
 */
public class AlterTableInterceptWriteHandler implements WriteHandler {
    private static final Logger LOG = Logger.getLogger(AlterTableInterceptWriteHandler.class);

    private final WriteCoordinator writeCoordinator;
    private final RowTransformer rowTransformer;
    private final byte[] newTableName;

    private RecordingCallBuffer<KVPair> recordingCallBuffer;

    public AlterTableInterceptWriteHandler(RowTransformer rowTransformer, byte[] newTableName) {
        this.writeCoordinator = SpliceDriver.driver().getTableWriter();
        this.rowTransformer = rowTransformer;
        this.newTableName = newTableName;
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        try {
            // Don't intercept deletes from the old table.
            if (mutation.getType() != KVPair.Type.DELETE) {
                KVPair newPair = rowTransformer.transform(mutation);
                initTargetCallBuffer(ctx).add(newPair);
                ctx.success(mutation);
            }
        } catch (Exception e) {
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }

        ctx.sendUpstream(mutation);
    }

    @Override
    public void next(List<KVPair> mutations, WriteContext ctx) {
        throw new UnsupportedOperationException("never called");
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        try {
            if (recordingCallBuffer != null) {
                recordingCallBuffer.flushBuffer();
            }
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw new IOException(e);
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        try {
            if (recordingCallBuffer != null) {
                recordingCallBuffer.close();
            }
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw new IOException(e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /* Only need to create the CallBuffer once, but not until we have a WriteContext */
    private RecordingCallBuffer<KVPair> initTargetCallBuffer(WriteContext ctx) {
        if (recordingCallBuffer == null) {
            recordingCallBuffer = writeCoordinator.writeBuffer(newTableName, ctx.getTxn());
        }
        return recordingCallBuffer;
    }
}
