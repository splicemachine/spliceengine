package com.splicemachine.derby.hbase;

import java.io.IOException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
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
	public long run(Scan scan,SpliceObserverInstructions instructions) throws IOException, StandardException;
}