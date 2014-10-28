package com.splicemachine.derby.impl.sql.execute;

import java.io.IOException;

import com.splicemachine.pipeline.writecontext.PipelineWriteContext;

/**
 * @author Scott Fines
 *         Date: 8/29/14
 */
public interface LocalWriteFactory {

    void addTo(PipelineWriteContext ctx,boolean keepState,int expectedWrites) throws IOException;

    long getConglomerateId();
}
