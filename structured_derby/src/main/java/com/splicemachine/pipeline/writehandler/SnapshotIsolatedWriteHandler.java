package com.splicemachine.pipeline.writehandler;

import java.io.IOException;
import java.util.List;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.log4j.Logger;

public class SnapshotIsolatedWriteHandler implements WriteHandler {
    private static final Logger LOG = Logger.getLogger(SnapshotIsolatedWriteHandler.class);

    private WriteHandler delegate;
    private DDLFilter ddlFilter;
    
    public SnapshotIsolatedWriteHandler(WriteHandler delegate, DDLFilter ddlFilter) {
        super();
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "instance");
        this.delegate = delegate;
        this.ddlFilter = ddlFilter;
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "next %s",mutation);
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
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "next %s",mutations);
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
    public void flush(WriteContext ctx) throws IOException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "flush");
        if (ddlFilter.isVisibleBy(ctx.getTxn())) {
            delegate.flush(ctx);
        }
    }

	@Override
	public void close(WriteContext ctx) throws IOException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "close");
		if (ddlFilter.isVisibleBy(ctx.getTxn())) {
            delegate.close(ctx);
        }		
	}
    
    

}
