package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.pipeline.context.PipelineWriteContext;

import java.io.IOException;

/**
 * Implementations configure a PipelineWriteContext for various constraints, indexes, states, etc.
 *
 * @author Scott Fines
 *         Date: 8/29/14
 */
public interface LocalWriteFactory {

    void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException;

    long getConglomerateId();

    boolean canReplace(LocalWriteFactory newContext);

    void replace(LocalWriteFactory newFactory);
}
