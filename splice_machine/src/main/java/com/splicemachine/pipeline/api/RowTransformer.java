package com.splicemachine.pipeline.api;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.hbase.KeyValue;

import com.splicemachine.hbase.KVPair;
/**
 * Transformer interface for taking a base row and transforming it to another representation.
 * 
 *
 */
public interface RowTransformer extends Closeable  {
	public KVPair transform(KeyValue kv) throws StandardException, SQLException, IOException;
}
