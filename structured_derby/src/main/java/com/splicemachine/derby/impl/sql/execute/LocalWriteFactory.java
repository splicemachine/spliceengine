package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.hbase.batch.PipelineWriteContext;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 8/29/14
 */
public interface LocalWriteFactory {

    void addTo(PipelineWriteContext ctx,boolean keepState,int expectedWrites) throws IOException;

    long getConglomerateId();
}
