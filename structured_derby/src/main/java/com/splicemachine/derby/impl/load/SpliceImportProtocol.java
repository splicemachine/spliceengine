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
	 * @param sourceFile the Source file to import
	 * @param locations the block locations to import
	 * @param destTable the name of the destination table
	 * @param delimiter the delimiter to split the file lines around
	 * @param columnTypes an array of column types. Each position in this array corresponds to the type
	 *                    in the split columns--e.g, if a file line looks like foo,bar,bat, then
	 *                    {@code columnTypes[0] = typeOf(foo),columnTypes[1] = typeOf(bar),columnTypes[2] = typeOf(bat)}
	 * @param activeCols which columns should be inserted, and which should be ignored
	 * @return the number of rows which were inserted as a result of importing all the block locations
	 * @throws IOException if something bad happens and we can't insert data.
	 */
	public long doImport(String sourceFile,
						Collection<BlockLocation> locations,
						String destTable,String delimiter,
						int[] columnTypes,FormatableBitSet activeCols) throws IOException;

	/**
	 * Imports all data in a given file, regardless of block-locations.
	 *
	 * This is not ideal, as it ignores data locality and essentially requires you to import the entire file
	 * sequentially, but is necessary when files are compressed using a non-splittable compression (such as gzip).
	 *
	 * @param sourceFile the Source file to import
	 * @param destTable the destination file
	 * @param delimiter the column delimiter to split around
	 * @param columnTypes the types of the columns
	 * @param activeCols the columns to import
	 * @return the number of rows which were inserted as a result of importing all the block locations
	 * @throws IOException if something bad happens and we can't import data.
	 */
	public long importFile(String sourceFile,String destTable,String delimiter,int[] columnTypes, FormatableBitSet activeCols) throws IOException;
}
