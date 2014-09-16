package com.splicemachine.derby.impl.sql.execute.index;

import java.io.IOException;
import java.util.List;

import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.batch.WriteHandler;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.si.impl.DDLFilter;
import org.apache.log4j.Logger;

public class SnapshotIsolatedWriteHandler implements WriteHandler {
    private static final Logger LOG = Logger.getLogger(SnapshotIsolatedWriteHandler.class);

    private WriteHandler delegate;
    private DDLFilter ddlFilter;
    
    public SnapshotIsolatedWriteHandler(WriteHandler delegate, DDLFilter ddlFilter) {
        super();
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
    public void next(List<KVPair> mutations, WriteContext ctx) {
        try {
            if (ddlFilter.isVisibleBy(ctx.getTxn())) {
                delegate.next(mutations, ctx);
            }else
								ctx.sendUpstream(mutations);
        } catch (IOException e) {
            LOG.error("Couldn't asses the visibility of the DDL operation", e);
            for (KVPair mutation : mutations) {
                ctx.failed(mutation, WriteResult.failed(e.getMessage()));
            }
        }
    }

    @Override
    public void finishWrites(WriteContext ctx) throws IOException {
        if (ddlFilter.isVisibleBy(ctx.getTxn())) {
            delegate.finishWrites(ctx);
        }
    }

}
