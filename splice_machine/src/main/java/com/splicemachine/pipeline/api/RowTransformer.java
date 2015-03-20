package com.splicemachine.pipeline.api;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.hbase.KVPair;
/**
 * Transformer interface for taking a base row and transforming it to another representation.
 * 
 *
 */
public interface RowTransformer extends Closeable  {
	public KVPair transform(KVPair kv) throws StandardException, SQLException, IOException;
}
