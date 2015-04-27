package com.splicemachine.pipeline.ddl;

import java.io.IOException;

import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.storage.EntryDecoder;

/**
 * Interface describing descriptors used for DDL operations that enforce transactionality
 * when altering tables by using the intercept/populate method.
 * <p/>
 * These descriptors can be thought of as factories that create components to facilitate
 * transforming original table rows to altered table rows and writing them to the new
 * conglomerate.
 *
 * @author Jeff Cunningham
 *         Date: 3/27/15
 */
public interface TransformingDDLDescriptor extends TentativeDDLDesc {

    /**
     * Create a RowTransformer that can map rows from the original table
     * to the altered table.
     * @return the appropriate RowTransformer
     * @throws IOException
     */
    RowTransformer createRowTransformer() throws IOException;

    /**
     * Create a WriteHandler that will use a RowTransformer to map and transform rows from
     * the original table to the altered table.
     * @param transformer the RowTransformer to use
     * @return the appropriate WriteHandler
     * @throws IOException
     */
    WriteHandler createWriteHandler(RowTransformer transformer) throws IOException;


    int[] getKeyColumnPositions();
}
