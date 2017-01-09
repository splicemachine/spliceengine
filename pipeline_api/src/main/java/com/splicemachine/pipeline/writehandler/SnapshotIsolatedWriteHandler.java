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

package com.splicemachine.pipeline.writehandler;

import java.io.IOException;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.storage.Record;
import org.apache.log4j.Logger;

public class SnapshotIsolatedWriteHandler implements WriteHandler {
    private static final Logger LOG = Logger.getLogger(SnapshotIsolatedWriteHandler.class);

    private WriteHandler delegate;
    private DDLFilter ddlFilter;
    
    public SnapshotIsolatedWriteHandler(WriteHandler delegate, DDLFilter ddlFilter) {
        this.delegate = delegate;
        this.ddlFilter = ddlFilter;
    }

    @Override
    public void next(Record mutation, WriteContext ctx) {
        try {
            if (ddlFilter.isVisibleBy(ctx.getTxn())) {
                delegate.next(mutation, ctx);
            }else
                ctx.sendUpstream(mutation);
        } catch (IOException e) {
            LOG.error("Couldn't asses the visibility of the DDL operation", e);
            ctx.failed(mutation, WriteResult.failed(e.getClass().getCanonicalName()+":"+e.getMessage()));
        }
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        if (ddlFilter.isVisibleBy(ctx.getTxn())) {
            delegate.flush(ctx);
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        if (ddlFilter.isVisibleBy(ctx.getTxn())) {
            delegate.close(ctx);
        }
    }

}
