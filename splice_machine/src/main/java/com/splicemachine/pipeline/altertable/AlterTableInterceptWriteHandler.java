package com.splicemachine.pipeline.altertable;

import java.io.IOException;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.RowTransformer;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import org.apache.log4j.Logger;

import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Jeff Cunningham
 *         Date: 3/16/15
 */
public class AlterTableInterceptWriteHandler implements WriteHandler{
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
    private RecordingCallBuffer<KVPair> initTargetCallBuffer(WriteContext ctx) throws IOException{
        if (recordingCallBuffer == null) {
            recordingCallBuffer = writeCoordinator.writeBuffer(ctx.remotePartition(newTableName), ctx.getTxn());
        }
        return recordingCallBuffer;
    }
}
