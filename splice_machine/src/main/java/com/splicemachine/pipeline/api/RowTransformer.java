package com.splicemachine.pipeline.api;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.hbase.KVPair;
/**
 * Transformer interface for taking a base row and transforming it to another representation.
 * 
 *
 */
public interface RowTransformer extends Closeable  {

    /**
     * Transform a source KVPair to a target KVPair, where a KVPair represents a
     * row in a table that's being altered. The table may have been altered by
     * adding or removing a column, or constraint.<br/>
     * This method is used during the intercept phase of alter table.
     * @param kvPair the row to transform.
     * @return the transformed row.
     * @throws StandardException
     * @throws IOException
     */
    KVPair transform(KVPair kvPair) throws StandardException, IOException;

    /**
     * Transform source KVPairs to a target KVPair. This method is used to squash
     * multiple version of the same KVPair and output the latest version.<br/>
     * This method is used during the populate phase of alter table, when copying
     * existing rows from the source conglomerate to the new target.
     * @param kvPairs the ordered list of KVPair versions of a given KVPair. The
     *                KVPairs are sorted from oldest to newest.
     * @return the squashed latest version of the row.
     * @throws StandardException
     * @throws IOException
     */
    KVPair transform(List<KVPair> kvPairs) throws StandardException, IOException;
}
