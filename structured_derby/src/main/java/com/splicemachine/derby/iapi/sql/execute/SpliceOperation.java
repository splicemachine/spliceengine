package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.stats.SinkStats;

import java.io.IOException;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

/**
 * 
 * Interface for Parallel Operations in the Splice Machine.
 * 
 * @author John Leach
 *
 */

public interface SpliceOperation extends NoPutResultSet {
	/**
	 * 
	 * Enumeration with the following types:
	 * 
	 * 	SCAN 	: Accesses HBase storage.
	 *  MAP		: Can be pushed to either the scan or the reduce Operation below it on the left hand side.
	 *  REDUCE	: The node needs to run the reduce steps after the sink.
	 *  SINK	: 
	 *  SCROLL	: The node returns a scrollable set of data to the client.
	 *  
	 */
	public enum NodeType { SCAN, MAP, REDUCE, SINK, SCROLL}
	
	/**
	 * Get the mechanism for providing Rows to the SpliceNoPutResultSet
	 * @return the mechanism for providing Rows to the SpliceNoPutResultSet
	 */
	public RowProvider getMapRowProvider(SpliceOperation top,ExecRow outputRowFormat);
	
	/**
	 * Get the mechanism for providing Rows to the SpliceNoPutResultSet
	 * @return the mechanism for providing Rows to the SpliceNoPutResultSet
	 */
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow outputRowFormat);
		
	/**
	 * Performs the traditional MapReduce shuffle operation for node types of SINK or REDUCE.
	 * 
	 * <p>Note: Implementations {@bold cannot} call {@code this.getNextRowCore()} from inside this
	 * method, as it may result in steps being repeated twice, which can cause incorrect results
	 * to be returned. The only exception
	 * 
	 * @param keyValues
	 * @return rows
     * @throws org.apache.hadoop.hbase.DoNotRetryIOException if an error occurs that is not retriable,
     * IOException for unexpected connectivity issues, etc.
	 */
	public SinkStats sink() throws IOException;
	/**
	 * Initializes the node with the statement and the language context from the SpliceEngine.
	 * 
	 * @param statement
	 * @param llc
	 */
	public void init(SpliceOperationContext operationContext);

	/**
	 * Cleanup any node external connections or resources.
	 * 
	 * @param statement
	 * @param llc
	 */
	public void cleanup();
	/**
	 * List of Node Types that determine the Operation's behaviour pattern.
	 * 
	 * @param statement
	 * @param llc
	 */
	public List<NodeType> getNodeTypes();
	/**
	 * Set of operations for a node.
	 * 
	 * @param statement
	 * @param llc
	 */
	public List<SpliceOperation> getSubOperations();
	/**
	 * Unique node sequence id.  Should move from Zookeeper to uuid generator.
	 * 
	 * @param statement
	 * @param llc
	 */
	public String getUniqueSequenceID();
	/**
	 * Execute a sink operation.  Must be a sink node.  This operation will be called from the OperationTree. 
	 * 
	 * @param statement
	 * @param llc
	 * 
	 * @see com.splicemachine.derby.impl.sql.execute.operations.OperationTree
	 */
	public void executeShuffle() throws StandardException;
	/**
	 * 
	 * Executes a scan operation from a node that has either a SCROLL node type or that is called from another node.
	 * 
	 * @return
	 */
	public NoPutResultSet executeScan() throws StandardException;
	/**
	 * 
	 * Probe scan for hash joins.  This may not belong in the interface and is just a once off.
	 * 
	 * @param activation
	 * @param operations
	 * @return
	 */
	public NoPutResultSet executeProbeScan() throws StandardException;
	/**
	 * 
	 * Gets the left Operation for a Operation.  They can be named different things in different operations (Source, LeftResultSet, etc.).  
	 * This gives a simple method to retrieve that operation.  This needs to be implemented in each operation.
	 * 
	 * @return
	 */
	public SpliceOperation getLeftOperation();
	/**
	 * 
	 * Recursively generates the left operation stack.  This method is implemented properly as long as you inherit from
	 * the SpliceBaseOperation.
	 * 
	 * @return
	 */
	public void generateLeftOperationStack(List<SpliceOperation> operations);

	/**
	 * 
	 * Gets the right Operation for a Operation.  They can be named different things in different operations (Source, LeftResultSet, etc.).  
	 * This gives a simple method to retrieve that operation.  This needs to be implemented in each operation.
	 * 
	 * @return
	 */
	public SpliceOperation getRightOperation();
	/**
	 * 
	 * Recursively generates the left operation stack.  This method is implemented properly as long as you inherit from
	 * the SpliceBaseOperation.
	 * 
	 * @return
	 */
	public void generateRightOperationStack(boolean initial, List<SpliceOperation> operations);
	/**
	 * 
	 * The outgoing field definition of the record.  Do we need incoming as well?
	 * 
	 * @return
	 */
	public ExecRow getExecRowDefinition();
}
