package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.pipeline.context.PipelineWriteContext;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface WriteFactoryGroup{

    void addFactory(LocalWriteFactory writeFactory);

    void addFactories(PipelineWriteContext context,boolean keepState,int expectedWrites) throws IOException;

    void replace(LocalWriteFactory newFactory);

    void clear();

    boolean isEmpty();
}
