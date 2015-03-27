package com.splicemachine.pipeline.ddl;

import java.io.IOException;

import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.api.WriteHandler;

/**
 * @author Jeff Cunningham
 *         Date: 3/27/15
 */
public interface TransformingDDLDescriptor extends TentativeDDLDesc {

    RowTransformer createRowTransformer() throws IOException;

    WriteHandler createWriteHandler(RowTransformer transformer) throws IOException;
}
