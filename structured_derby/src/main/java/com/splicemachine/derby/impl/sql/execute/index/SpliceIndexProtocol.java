package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.derby.stats.SinkStats;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;

/**
 * Protocol for performing maintenance tasks on an Indexed table.
 *
 * @author Scott Fines
 * Created on: 3/7/13
 */
public interface SpliceIndexProtocol extends CoprocessorProtocol{

    /**
     * Build an index using the specified fields.
     *
     * When an index is first created, it's possible (likely, even) that there is
     * already data in the table itself. Thus, we call this method to construct the
     * index in a parallel fashion.
     *
     * @param indexConglomId the conglomerate id of the index to build
     * @param baseConglomId the conglomerate id of the main table
     * @param indexColsToBaseColMap a mapping between index columns and the base column mapping
     * @return Statistics information about how long this region took to update the index
     * @throws IOException if something goes wrong
     */
    public SinkStats buildIndex(long indexConglomId,
                                long baseConglomId,
                                int[] indexColsToBaseColMap,boolean isUnique) throws IOException;

    /**
     * Drop the specified index from the specified table.
     *
     * @param indexConglomId the conglomerate id of the index to drop
     * @param baseConglomId the conglomerate id of the table to drop the index off of
     * @throws IOException if something goes wrong dropping the index.
     */
    public void dropIndex(long indexConglomId,long baseConglomId) throws IOException;
}
