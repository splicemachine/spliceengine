package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.derby.stats.SinkStats;
import org.apache.derby.iapi.sql.Activation;
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
     * Drop the specified index from the specified table.
     *
     * @param indexConglomId the conglomerate id of the index to drop
     * @param baseConglomId the conglomerate id of the table to drop the index off of
     * @throws IOException if something goes wrong dropping the index.
     */
    public void dropIndex(long indexConglomId,long baseConglomId) throws IOException;
}
