package com.splicemachine.pipeline.writehandler;

import java.io.IOException;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.si.impl.DDLFilter;

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
    public void next(KVPair mutation, WriteContext ctx) {
        try {
            if (ddlFilter.isVisibleBy(ctx.getTxn())) {
                delegate.next(mutation, ctx);
            }else
                ctx.sendUpstream(mutation);
        } catch (IOException e) {
            LOG.error("Couldn't asses the visibility of the DDL operation", e);
            ctx.failed(mutation, WriteResult.failed(e.getMessage()));
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
