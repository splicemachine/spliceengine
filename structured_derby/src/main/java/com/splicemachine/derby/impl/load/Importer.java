package com.splicemachine.derby.impl.load;

import com.splicemachine.hbase.writer.WriteStats;
import com.splicemachine.stats.IOStats;
import com.splicemachine.stats.TimeView;

import java.io.Closeable;

/**
 * Accepts, Processes, and Writes rows that are fed to it.
 *
 * @author Scott Fines
 * Created on: 9/30/13
 */
public interface Importer extends Closeable {

    void process(String[] parsedRow) throws Exception;

		boolean processBatch(String[]...parsedRows) throws Exception;

    boolean isClosed();

		WriteStats getWriteStats();

		TimeView getTotalTime();
}
