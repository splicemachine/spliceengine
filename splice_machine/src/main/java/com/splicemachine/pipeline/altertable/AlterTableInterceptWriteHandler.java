/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.pipeline.altertable;

import java.io.IOException;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.RowTransformer;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public AlterTableInterceptWriteHandler(RowTransformer rowTransformer, byte[] newTableName) {
        this.writeCoordinator = PipelineDriver.driver().writeCoordinator();
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
