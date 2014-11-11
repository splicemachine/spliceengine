package com.splicemachine.derby.hbase;

import com.splicemachine.derby.stats.TaskStats;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;
/**
 * Interface for SpliceOperationCoprocessor
 * 
 * @author johnleach
 *
 */
public interface SpliceOperationProtocol extends CoprocessorProtocol {
	/**
	 * Method to run the topOperations sink method in parallel
	 * 
	 * @param statement
	 * @param scan
	 * @param topOperation
	 * @return
	 * @throws IOException
	 * @throws StandardException
	 */
	public TaskStats run(Scan scan,SpliceObserverInstructions instructions) throws IOException, StandardException;
}