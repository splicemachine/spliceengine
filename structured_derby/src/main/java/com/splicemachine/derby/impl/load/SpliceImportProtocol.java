package com.splicemachine.derby.impl.load;

import java.io.IOException;
import java.util.Collection;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * Protocol for coprocessors which import data into splice.
 */
public interface SpliceImportProtocol extends CoprocessorProtocol{

	/**
	 * Imports all the lines contained in the BlockLocations passed to it.
	 *
	 * @param locations the block locations to import
	 * @param context the context of this import.
	 * @return the number of rows which were inserted as a result of importing all the block locations
	 * @throws IOException if something bad happens and we can't insert data.
	 */
	public long doImport(Collection<BlockLocation> locations, ImportContext context) throws IOException;

	/**
	 * Imports all data in a given file, regardless of block-locations.
	 *
	 * This is not ideal, as it ignores data locality and essentially requires you to import the entire file
	 * sequentially, but is necessary when files are compressed using a non-splittable compression (such as gzip).
	 *
	 * @param context the context of this import request.
	 * @return the number of rows which were inserted as a result of importing all the block locations
	 * @throws IOException if something bad happens and we can't import data.
	 */
	public long importFile( ImportContext context) throws IOException;
}
