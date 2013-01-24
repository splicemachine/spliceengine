package com.splicemachine.derby.impl.load;

import java.io.IOException;
import java.util.Collection;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface SpliceImportProtocol extends CoprocessorProtocol{

	public long doImport(String sourceFile,
						Collection<BlockLocation> locations,
						String destTable,String delimiter,
						int[] columnTypes,FormatableBitSet activeCols) throws IOException;
}
