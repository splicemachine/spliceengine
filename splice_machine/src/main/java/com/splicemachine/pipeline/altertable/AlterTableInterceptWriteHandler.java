/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.pipeline.altertable;

import java.io.IOException;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.RowTransformer;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;
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

    private RecordingCallBuffer<Record> recordingCallBuffer;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public AlterTableInterceptWriteHandler(RowTransformer rowTransformer, byte[] newTableName) {
        this.writeCoordinator = PipelineDriver.driver().writeCoordinator();
        this.rowTransformer = rowTransformer;
        this.newTableName = newTableName;
    }

    @Override
    public void next(Record mutation, WriteContext ctx) {
        try {
            // Don't intercept deletes from the old table.
            if (mutation.getRecordType() != RecordType.DELETE) {
                Record newPair = rowTransformer.transform(mutation);
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
    private RecordingCallBuffer<Record> initTargetCallBuffer(WriteContext ctx) throws IOException{
        if (recordingCallBuffer == null) {
            recordingCallBuffer = writeCoordinator.writeBuffer(ctx.remotePartition(newTableName), ctx.getTxn());
        }
        return recordingCallBuffer;
    }
}
