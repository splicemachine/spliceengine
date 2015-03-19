package com.splicemachine.pipeline.writehandler.altertable;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.altertable.AddColumnRowTransformer;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Jeff Cunningham
 *         Date: 3/16/15
 */
public class AddColumnInterceptWriteHandler implements WriteHandler {
    private static final Logger LOG = Logger.getLogger(AddColumnInterceptWriteHandler.class);

    private final WriteCoordinator writeCoordinator;
    private final AddColumnRowTransformer rowTransformer;
    private final byte[] newTableName;

    private RecordingCallBuffer<KVPair> recordingCallBuffer;

    public AddColumnInterceptWriteHandler(AddColumnRowTransformer rowTransformer, byte[] newTableName) {
        this.writeCoordinator = SpliceDriver.driver().getTableWriter();
        this.rowTransformer = rowTransformer;
        this.newTableName = newTableName;
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        try {
            KVPair newPair;
            if (shouldIntercept(mutation.getType())) {
                newPair = mutation;
            } else {
                KeyValue kv = new KeyValue(mutation.getRowKey(), SpliceConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES,mutation.getValue());
                newPair = rowTransformer.transform(kv);
            }
            initTargetCallBuffer(ctx).add(newPair);
            ctx.success(mutation);
        } catch (Exception e) {
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }

        ctx.sendUpstream(mutation);
    }

    /* This WriteHandler doesn't do anything when, for example, we delete from the old table. */
    private boolean shouldIntercept(KVPair.Type type) {
        return type == KVPair.Type.INSERT || type == KVPair.Type.UPDATE || type == KVPair.Type.UPSERT;
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
