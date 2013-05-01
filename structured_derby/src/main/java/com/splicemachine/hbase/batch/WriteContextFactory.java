package com.splicemachine.hbase.batch;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteContextFactory<T> {

    WriteContext create(T key) throws IOException, InterruptedException;

    void dropIndex(long indexConglomId);

    void addIndex(long indexConglomId, int[] indexColsToBaseColMap, boolean unique);
}
