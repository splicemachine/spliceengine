package com.splicemachine.derby.hbase;

import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;

import com.splicemachine.derby.stats.TaskStats;
/**
 * Interface for SpliceOperationCoprocessor
 * 
 * @author johnleach
 *
 */
public interface SpliceOperationService extends CoprocessorService {
	/**
	 * Method to run the topOperations sink method in parallel
	 * 
	 * @param scan
	 * @param instructions
	 * @return
	 * @throws IOException
	 * @throws StandardException
	 */
	public TaskStats run(Scan scan,SpliceObserverInstructions instructions) throws IOException, StandardException;
}