package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.writecontext.PipelineWriteContext;

import java.io.IOException;

/**
 * Implementations configure a PipelineWriteContext for various constraints, indexes, states, etc.
 *
 * @author Scott Fines
 *         Date: 8/29/14
 */
interface LocalWriteFactory {

    void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException;

    long getConglomerateId();
}
