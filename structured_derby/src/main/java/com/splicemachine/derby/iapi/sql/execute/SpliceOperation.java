package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.derby.iapi.storage.RowProvider;

import java.io.IOException;
import java.util.List;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.RowLocation;

/**
 * 
 * Interface for Parallel Operations in the Splice Machine.
 * 
 * @author John Leach
 *
 */

public interface SpliceOperation  {

    RowLocation getCurrentRowLocation();

    void setCurrentRowLocation(RowLocation rowLocation);

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

    public int modifiedRowCount();

    public Activation getActivation();

    public void clearCurrentRow();

    public void close() throws StandardException,IOException;

    public void markAsTopResultSet();

    public void open() throws StandardException,IOException;

    public int resultSetNumber();

    public void setCurrentRow(ExecRow row);

    public ExecRow nextRow() throws StandardException,IOException;

	/**
	 * Get the mechanism for providing Rows to the SpliceNoPutResultSet
	 * @return the mechanism for providing Rows to the SpliceNoPutResultSet
	 */
	public RowProvider getMapRowProvider(SpliceOperation top,RowDecoder decoder) throws StandardException;
	
	/**
	 * Get the mechanism for providing Rows to the SpliceNoPutResultSet
	 * @return the mechanism for providing Rows to the SpliceNoPutResultSet
	 */
	public RowProvider getReduceRowProvider(SpliceOperation top,RowDecoder decoder) throws StandardException;

    /**
     * Encoder for writing ExecRows into HBase (temp table or other location).
     *
     * @return
     * @throws StandardException
     */
    public RowEncoder getRowEncoder() throws StandardException;
	/**
	 * Initializes the node with the statement and the language context from the SpliceEngine.
	 * 
	 * @throws StandardException
	 */
	public void init(SpliceOperationContext operationContext) throws StandardException;

	/**
	 * List of Node Types that determine the Operation's behaviour pattern.
	 * 
	 */
	public List<NodeType> getNodeTypes();

	/**
	 * Set of operations for a node.
	 * 
	 */
	public List<SpliceOperation> getSubOperations();

	/**
	 * Unique node sequence id.  Should move from Zookeeper to uuid generator.
	 * 
	 */
	public String getUniqueSequenceID();
	/**
	 * Execute a sink operation.  Must be a sink node.  This operation will be called from the OperationTree. 
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
	 * @throws StandardException 
	 */

	public ExecRow getExecRowDefinition() throws StandardException;

    /**
     * Returns an array containing integer pointers into the columns of the table. This
     * array relates the compact row column location (integer pointer) to the original
     * location (the nth column on the original table).
     *
     *
     * @param tableNumber
     * @return
     */
    int[] getRootAccessedCols(long tableNumber);

    /**
     * Returns true if this operation references the given table number.  For a join,
     * this means either the left side or the right side involves that table.  For things
     * like table scans, it's true if it is scanning that table.
     */
    boolean isReferencingTable(long tableNumber);

    /**
     * Prints out a string representation of this operation, formatted for easy human consumption.
     *
     * @return a pretty-printed string representation of this operation.
     */
    String prettyPrint(int indentLevel);
}
