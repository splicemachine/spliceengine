package com.splicemachine.derby.impl.sql.execute.index;

import java.io.IOException;

import com.google.protobuf.Service;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;

/**
 * Protocol for performing maintenance tasks on an Indexed table.
 *
 * @author Scott Fines
 * Created on: 3/7/13
 */
public interface SpliceIndexService extends CoprocessorService, Service {

    /**
     * Drop the specified index from the specified table.
     *
     * @param indexConglomId the conglomerate id of the index to drop
     * @param baseConglomId the conglomerate id of the table to drop the index off of
     * @throws IOException if something goes wrong dropping the index.
     */
    public void dropIndex(long indexConglomId,long baseConglomId) throws IOException;
}
